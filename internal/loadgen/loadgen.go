package loadgen

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
    "github.com/thanhpk/randstr"

	"PGLoadBalance/internal/monitoring"

)

type modeEnum int

const (
	avgTupleLen          = 1060
	insertMode  modeEnum = iota
	deleteMode
	updateMode
)

type LoadGen struct {
	pool             *pgxpool.Pool
	mon              *monitoring.Monitor
	minSizeMB        int64
	maxSizeMB        int64
	tableName        string
	rand             *rand.Rand
	emptyTuplesBytes atomic.Int64
	liveTupleBytes   atomic.Int64
	deadTupleBytes   atomic.Int64
	totalRelSize     atomic.Int64
	delta            atomic.Int64 // общий объём бд - totalRelSIze
}

func New(pool *pgxpool.Pool, mon *monitoring.Monitor, minSize, maxSize int64) *LoadGen {
	return &LoadGen{
		pool:      pool,
		mon:       mon,
		minSizeMB: minSize,
		maxSizeMB: maxSize,
		tableName: "test",
		rand:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (lg *LoadGen) Run(ctx context.Context) {
	_, err := lg.pool.Exec(ctx,
		fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id bigserial PRIMARY KEY,
			data text
		)`, lg.tableName))
	if err != nil {
		log.Printf("Failed to create table: %v", err)
		return
	}
	_, err = lg.pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS pgstattuple`)
	if err != nil {
		log.Printf("Failed to create pgstattuple extension: %v", err)
	}

	lg.pool.Exec(ctx, fmt.Sprintf(
		`ALTER TABLE %s 
		SET (
			autovacuum_enabled = off,
			toast.autovacuum_enabled = off
		)`, lg.tableName))

	ticker := time.NewTicker(333 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			dbSizeMB, err := lg.mon.GetDBSize()
			if err != nil {
				log.Printf("Failed to get DB size: %v", err)
				continue
			}

			log.Printf("DB: %d\n", dbSizeMB)
            

			// Получаем и сохраняем общий размер таблицы
			var totalSize int64
			err = lg.pool.QueryRow(ctx,
				"SELECT pg_total_relation_size($1)",
				lg.tableName,
			).Scan(&totalSize)
			if err != nil {
				log.Printf("Error to get totalsize")

			}
			lg.totalRelSize.Store(totalSize)
			log.Printf("TOTALSIZE: %d", totalSize)
			lg.delta.Store(dbSizeMB*1024*1024 - lg.totalRelSize.Load())
			lg.checkMaintenance(ctx)

			mode := insertMode

			if dbSizeMB >= lg.maxSizeMB {
				mode = deleteMode
			} else if dbSizeMB <= lg.minSizeMB {
				mode = insertMode
			} else {

				switch lg.rand.Intn(3) {
				case 0:
					mode = insertMode
				case 1:
					mode = deleteMode
				case 2:
					mode = updateMode
				}
			}

			switch mode {
			case insertMode:
				lg.batchInsert(ctx)
			case deleteMode:
				lg.randomDelete(ctx)
			case updateMode:
				lg.batchUpdate(ctx)
			}
		}
	}
}

func (lg *LoadGen) batchInsert(ctx context.Context) error {

	// Получаем текущий размер таблицы в байтах
	currentSizeBytes := lg.totalRelSize.Load() + lg.delta.Load()
	maxSizeBytes := lg.maxSizeMB * 1024 * 1024

	if maxSizeBytes <= currentSizeBytes {
		return nil
	}
	// Рассчитываем доступное пространство
	freeSpaceBytes := maxSizeBytes - currentSizeBytes + lg.emptyTuplesBytes.Load()

	if freeSpaceBytes <= 0 {
		log.Println("No space available for insert")
		return nil
	}

	// Рассчитываем максимальное количество строк для вставки
	maxInsertRows := int(freeSpaceBytes / avgTupleLen)
	log.Printf("maxInsertrows: %d", maxInsertRows)
	if maxInsertRows <= 0 {
		return nil
	}

	// Ограничиваем размер пакета
	if maxInsertRows > 5000 {
		maxInsertRows = 5000
	}

	// Генерируем случайное количество строк (от 1 до maxInsertRows)
	rowsToInsert := 1
	if maxInsertRows > 1 {
		rowsToInsert = lg.rand.Intn(maxInsertRows) + 1
	}

	batch := &pgx.Batch{}
	for i := 0; i < rowsToInsert; i++ {
		batch.Queue(
			fmt.Sprintf("INSERT INTO %s(data) VALUES($1)", lg.tableName),
			randstr.String(1024), // Генерируем строку 1KB
		)
	}

	if err := lg.pool.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("batch insert failed: %w", err)
	}

	log.Printf("Inserted rows: %d", rowsToInsert)
	return nil

}

func (lg *LoadGen) randomDelete(ctx context.Context) error {

	currentSize := lg.totalRelSize.Load() + lg.delta.Load()

	toDeleteBytesInt64 := currentSize - (lg.minSizeMB * 1024 * 1024) - lg.deadTupleBytes.Load() - lg.emptyTuplesBytes.Load()

	toDeleteTuplesFloat := float64(toDeleteBytesInt64) / float64(avgTupleLen)
	toDeleteTuples := int(toDeleteTuplesFloat)

	if toDeleteTuples <= 0 {
		toDeleteTuples = 0
	} else if toDeleteTuples >= 5000 {
		toDeleteTuples = 5000
	}

	rowsToDelete := lg.rand.Intn(int(toDeleteTuples) + 1)
	if rowsToDelete == 0 {
		return nil
	}

	res, err := lg.pool.Exec(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE id IN (SELECT id FROM %s ORDER BY id LIMIT %d)",
			lg.tableName, lg.tableName, rowsToDelete),
	)
	if err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}

	affected := res.RowsAffected()

	log.Printf("Deleted rows: %d", affected)

	return nil
}

func (lg *LoadGen) batchUpdate(ctx context.Context) error {
	freeBytes := lg.emptyTuplesBytes.Load()

	// Получаем количество живых строк
	var liveTupleCount int64
	if err := lg.pool.QueryRow(ctx,
		fmt.Sprintf("SELECT count(*) FROM %s", lg.tableName)).
		Scan(&liveTupleCount); err != nil {
		return fmt.Errorf("failed to get live tuple count: %w", err)
	}

	// Рассчитываем ограничения
	rowsBySpace := freeBytes / int64(avgTupleLen)
	rowsByLive := liveTupleCount
	maxRows := rowsBySpace
	if rowsByLive < maxRows {
		maxRows = rowsByLive
	}

	maxRows = maxRows + ((lg.liveTupleBytes.Load() / avgTupleLen) / 2)
	if maxRows > 5000 {
		maxRows = 5000
	}

	// Диагностический лог
	log.Printf(
		"batchUpdate diagnostics — freeBytes=%d avgTupleSize=%d liveTuples=%d -> maxRows=%d",
		freeBytes, avgTupleLen, liveTupleCount, maxRows,
	)

	if maxRows <= 0 {
		log.Printf("batchUpdate: no work to do (maxRows=%d)", maxRows)
		return nil
	}

	rowsToUpdate := int(maxRows)
	if rowsToUpdate > 1 {
		rowsToUpdate = lg.rand.Intn(rowsToUpdate) + 1
	}

	// Ещё один лог-пристрел перед Exec
	log.Printf("batchUpdate: going to update %d rows", rowsToUpdate)

	// Упрощенный UPDATE без no_hot
	res, err := lg.pool.Exec(ctx,
		fmt.Sprintf(`
            UPDATE %s 
            SET data = $1
            WHERE id IN (
                SELECT id FROM %s 
                ORDER BY random() 
                LIMIT %d
            )`,
			lg.tableName, lg.tableName, rowsToUpdate,
		),
		randstr.String(1024),
	)
	if err != nil {
		log.Printf("batchUpdate: Exec failed: %v", err)
		return err
	}

	affected := res.RowsAffected()
	log.Printf("Requested to update: %d, actually updated: %d", rowsToUpdate, affected)

	if affected == 0 {
		log.Printf("batchUpdate: zero rows updated — возможно, таблица пуста или нет подходящих id")
	}

	return nil
}

func (lg *LoadGen) checkMaintenance(ctx context.Context) {
	tupBytes, deadBytes, freeBytes, err := lg.getPgstattupleStats(ctx)
	if err != nil {
		log.Printf("pgstattuple failed: %v", err)
		return
	}
	lg.liveTupleBytes.Store(tupBytes)
	lg.emptyTuplesBytes.Store(freeBytes)
	lg.deadTupleBytes.Store(deadBytes)

	log.Printf(
		"LiveTupleBytes=%dB, DeadBytes=%dB, FreeBytes=%dB",
		tupBytes,
		deadBytes,
		freeBytes,
	)

	if deadBytes/avgTupleLen >= 1000 {
		log.Printf("Trigger manual VACUUM: dead tuples=%d", deadBytes)
		if _, err := lg.pool.Exec(ctx, fmt.Sprintf("VACUUM %s", lg.tableName)); err != nil {
			log.Printf("VACUUM failed: %v", err)
		}
	}

	rangeSize := (lg.maxSizeMB - lg.minSizeMB) * 1024 * 1024
	if float64(rangeSize)*0.8 <= float64(freeBytes) {
		log.Printf("checkMaintenance: запуск предварительной очистки")

		// VACUUM FULL
		if _, err := lg.pool.Exec(ctx, fmt.Sprintf("VACUUM FULL %s", lg.tableName)); err != nil {
			log.Printf("VACUUM FULL failed: %v", err)
		} else {
			log.Printf("VACUUM FULL выполнен")
			// обновление статистики
			tupBytes, deadBytes, freeBytes, err := lg.getPgstattupleStats(ctx)
			if err != nil {
				log.Printf("pgstattuple failed: %v", err)
				return
			}
			lg.liveTupleBytes.Store(tupBytes)
			lg.emptyTuplesBytes.Store(freeBytes)
			lg.deadTupleBytes.Store(deadBytes)

			log.Printf(
				"LiveTupleBytes=%dB, DeadBytes=%dB, FreeBytes=%dB",
				tupBytes,
				deadBytes,
				freeBytes,
			)
			dbSizeMB, err := lg.mon.GetDBSize()
			if err != nil {
				log.Printf("Failed to get DB size: %v", err)
			}

			log.Printf("DB: %d\n", dbSizeMB)

			// Получаем и сохраняем общий размер таблицы
			var totalSize int64
			err = lg.pool.QueryRow(ctx,
				"SELECT pg_total_relation_size($1)",
				lg.tableName,
			).Scan(&totalSize)
			if err != nil {
				log.Printf("AAAAAAAAAAAA")

			}
			lg.totalRelSize.Store(totalSize)
			log.Printf("TOTAL: %d", totalSize)
			lg.delta.Store(dbSizeMB*1024*1024 - lg.totalRelSize.Load())

		}
	}
}

func (lg *LoadGen) getPgstattupleStats(ctx context.Context) (
	tupleLenBytes int64,
	deadLenBytes int64,
	freeSpaceBytes int64,
	err error,
) {

	var tableLen int64
	var deadCount int64
	var localTupleCount int64
	var tuplePct float64
	var deadPct float64
	var freePct float64

	query := fmt.Sprintf(`
        SELECT
            table_len,
            tuple_count,    
            tuple_len,
            tuple_percent,  
            dead_tuple_count, 
            dead_tuple_len,
            dead_tuple_percent, 
            free_space,
            free_percent      
        FROM pgstattuple('%s')`, lg.tableName)

	err = lg.pool.QueryRow(ctx, query).Scan(
		&tableLen,
		&localTupleCount,
		&tupleLenBytes,
		&tuplePct,
		&deadCount,
		&deadLenBytes,
		&deadPct,
		&freeSpaceBytes,
		&freePct,
	)
	if err != nil {

		return 0, 0, 0, fmt.Errorf("pgstattuple scan failed: %w", err)
	}

	return tupleLenBytes, deadLenBytes, freeSpaceBytes, nil
}
