package loadgen

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"
	"PGLoadBalance/internal/monitoring"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type modeEnum int

const (
	insertMode modeEnum = iota
	deleteMode
	updateMode
)

type LoadGen struct {
	pool             *pgxpool.Pool
	mon              *monitoring.Monitoring
	minSizeMB        int64
	maxSizeMB        int64
	tableName        string
	rand             *rand.Rand
	countDeadTuples  atomic.Int64 
	countEmptyTuples atomic.Int64 // bytes
	avgTupleLen      atomic.Int64 
	tableLenBytes    atomic.Int64
	deadTupleBytes   atomic.Int64
}

func New(pool *pgxpool.Pool, mon *monitoring.Monitoring, minSize, maxSize int64) *LoadGen {
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
    // lg.initPartitioning(ctx)
	// Create table with no_hot column to disable HOT updates
	_, err := lg.pool.Exec(ctx,
		fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id bigserial PRIMARY KEY,
			data text,
			no_hot boolean DEFAULT false
		)`, lg.tableName))
	if err != nil {
		log.Printf("Failed to create table: %v", err)
		return
	}
	// Create index on no_hot to force non-HOT updates
	lg.pool.Exec(ctx, fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_no_hot ON %s(no_hot)", lg.tableName, lg.tableName))

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
			szy, err := lg.mon.GetDBSize()
			if err != nil {
				log.Printf("Failed to get DB size: %v", err)
				continue
			}

            log.Printf("DB: %d\n", szy)
                lg.checkMaintenance(ctx)

            sz := lg.tableLenBytes.Load() / (1024 * 1024) // Конвертируем байты в мегабайты

			mode := insertMode
			// В основном цикле обработки
                if sz >= lg.maxSizeMB || szy >= lg.maxSizeMB {
                    mode = deleteMode
                } else if sz <= lg.minSizeMB || szy <= lg.minSizeMB {
                    mode = insertMode
                } else {
                    // Случайный выбор между insert/update/delete
                    switch lg.rand.Intn(3) {
                    case 0:
                        mode = insertMode
                    case 1:
                        mode = deleteMode
                    case 2:
                        mode = updateMode
                    }
                }
			// В основной цикл добавляем ветку для updateMode
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
    currentSizeBytes := lg.tableLenBytes.Load()
    maxSizeBytes := lg.maxSizeMB * 1024 * 1024
    
    if maxSizeBytes <= currentSizeBytes {
        return nil
    }
    // Рассчитываем доступное пространство
    freeSpaceBytes := maxSizeBytes - currentSizeBytes + lg.countEmptyTuples.Load()
    
    
    if freeSpaceBytes <= 0 {
        log.Println("No space available for insert")
        return nil
    }

    // Получаем средний размер строки
       var avgLen int64
        // Если статистика недоступна, используем консервативную оценку
        avgLen = 1024 // 1KB по умолчанию

    // Рассчитываем максимальное количество строк для вставки
    maxInsertRows := int(freeSpaceBytes / avgLen)
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
            randString(1024, lg.rand), // Генерируем строку 1KB
        )
    }

    if err := lg.pool.SendBatch(ctx, batch).Close(); err != nil {
        return fmt.Errorf("batch insert failed: %w", err)
    }

    log.Printf("Inserted rows: %d", rowsToInsert)
    return nil

  
}

func (lg *LoadGen) randomDelete(ctx context.Context) error {

	freeBits := lg.countEmptyTuples.Load()
	//avgLenTuples := lg.avgTupleLen.Load()
	deadTupleBytes := lg.deadTupleBytes.Load()
    currentSize := lg.tableLenBytes.Load()

    var avgLenTuples int64
    avgLenTuples = 1024

	availableToDeleteBytesInt64 := currentSize - (lg.minSizeMB * 1024 * 1024) - deadTupleBytes - freeBits
    /*2025/07/24 05:41:29 TableSize=300498944B, LiveTupleBytes=103058113B, DeadCount=9, DeadBytes=9549B, FreeBytes=195631028B
2025/07/24 05:41:29 DB: 302 - зацикливание
     тут minSizeMB БОЛЬШЕ чем кол-во живых байт строк!!!!!!!!
     */
    availableToDeleteBytesFloat := float64(availableToDeleteBytesInt64) / float64(avgLenTuples)
     availableToDeleteBytes := int(availableToDeleteBytesFloat)


    // if ((lg.minSizeMB*1024*1024) >= (deadTupleBytes+freeBits)) {
    //     availableToDeleteBytes =  1000 -  int(lg.countDeadTuples.Load())//1000 ЭТО ПОРОГ ДЛЯ ДЭД VACUUM чтобы не зацикливалось
    //     log.Printf("ААААААААААААААААААААААААААААААААААа")
    //} else 
    if availableToDeleteBytes <= 0 {
		availableToDeleteBytes = 0
	} else  if availableToDeleteBytes >= 5000 {
		availableToDeleteBytes = 5000
	}
    

	rowsToDelete := lg.rand.Intn(int(availableToDeleteBytes) + 1)
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
    freeBytes := lg.countEmptyTuples.Load()
    avgTupleSize := 1024
   

    // Получаем количество живых строк
    var liveTupleCount int64
    if err := lg.pool.QueryRow(ctx,
        fmt.Sprintf("SELECT count(*) FROM %s", lg.tableName)).
        Scan(&liveTupleCount); err != nil {
        return fmt.Errorf("failed to get live tuple count: %w", err)
    }

    // Рассчитываем ограничения
    rowsBySpace := freeBytes / int64 (avgTupleSize)
    rowsByLive := liveTupleCount
    maxRows := rowsBySpace
    if rowsByLive < maxRows {
        maxRows = rowsByLive
    }
    if maxRows > 5000 {
        maxRows = 5000
    }

    // Диагностический лог
    log.Printf(
        "batchUpdate diagnostics — freeBytes=%d avgTupleSize=%d liveTuples=%d -> maxRows=%d",
        freeBytes, avgTupleSize, liveTupleCount, maxRows,
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

    res, err := lg.pool.Exec(ctx,
        fmt.Sprintf(`
            UPDATE %s 
            SET data = $1, no_hot = true 
            WHERE id IN (
                SELECT id FROM %s 
                ORDER BY random() 
                LIMIT %d
            )`,
            lg.tableName, lg.tableName, rowsToUpdate,
        ),
        randString(1024, lg.rand),
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
	tupBytes, deadCount, deadBytes, freeBytes, err := lg.getPgstattupleStats(ctx)
	if err != nil {
		log.Printf("pgstattuple failed: %v", err)
		return
	}

	// Save metrics to atomic fields
	lg.avgTupleLen.Store(tupBytes)
	lg.countDeadTuples.Store(deadCount)
	lg.countEmptyTuples.Store(freeBytes)
    lg.deadTupleBytes.Store(deadBytes)
	// tableLenBytes is already saved in getPgstattupleStats

	log.Printf(
		"TableSize=%dB, LiveTupleBytes=%dB, DeadCount=%d, DeadBytes=%dB, FreeBytes=%dB",
		lg.tableLenBytes.Load(),
		tupBytes,
		deadCount,
		deadBytes,
		freeBytes,
	)

	if deadCount >= 1000 {
		log.Printf("Trigger manual VACUUM: dead tuples=%d", deadCount)
		if _, err := lg.pool.Exec(ctx, fmt.Sprintf("VACUUM %s", lg.tableName)); err != nil {
			log.Printf("VACUUM failed: %v", err)
		}
	}

        // Сохраняем метрики
        // currentSize := lg.tableLenBytes.Load()
        // reclaimable := deadBytes + freeBytes
         // VACUUM FULL, если освободилось слишком много места
        rangeSize := (lg.maxSizeMB - lg.minSizeMB) * 1024 * 1024
        if float64(rangeSize)*0.8 <= float64(freeBytes) {
        // Сначала удаляем живые строки до minSizeMB
        if err := lg.preVacuumDelete(ctx, tupBytes); err != nil {
            log.Printf("preVacuumDelete error: %v", err)
        }
        // Потом VACUUM FULL
        if _, err := lg.pool.Exec(ctx, fmt.Sprintf("VACUUM FULL %s", lg.tableName)); err != nil {
            log.Printf("VACUUM FULL failed: %v", err)
        } else {
            log.Printf("VACUUM FULL executed")
        }
    }


}
   


func randString(n int, r *rand.Rand) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[r.Intn(len(letters))]
	}
	return string(b)
}

func (lg *LoadGen) getPgstattupleStats(ctx context.Context) (
	tupleLenBytes   int64,
	deadCount       int64,
	deadLenBytes    int64,
	freeSpaceBytes  int64,
	err             error,
) {
	// Temporary variables for scanning
	var tableLen    int64
	var tupleCount  int64
	var tuplePct    float64
	var deadPct     float64
	var freePct     float64


	// Query pgstattuple
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

	// Scan results
	err = lg.pool.QueryRow(ctx, query).Scan(
		&tableLen,        // table_len
		&tupleCount,      // tuple_count
		&tupleLenBytes,   // tuple_len
		&tuplePct,        // tuple_percent
		&deadCount,       // dead_tuple_count
		&deadLenBytes,    // dead_tuple_len
		&deadPct,         // dead_tuple_percent
		&freeSpaceBytes,  // free_space
		&freePct,         // free_percent
	)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("pgstattuple scan failed: %w", err)
	}

	// Save table size for future use
	lg.tableLenBytes.Store(tableLen)

	return tupleLenBytes, deadCount, deadLenBytes, freeSpaceBytes, nil
}


// Добавляем метод для предварительного удаления живых строк
func (lg *LoadGen) preVacuumDelete(ctx context.Context, liveBytes int64) error {
    // Сколько байт нужно убрать, чтобы остаться в minSizeMB
    minSizeBytes := lg.minSizeMB * 1024 * 1024
    bytesToRemove := liveBytes - minSizeBytes
    if bytesToRemove <= 0 {
        return nil
    }

    // Оценка среднего размера кортежа
    avgTupleSize := lg.avgTupleLen.Load()
    if avgTupleSize == 0 {
        avgTupleSize = 1024
    }

    rowsToDelete := int(bytesToRemove / avgTupleSize)
    if rowsToDelete <= 0 {
        return nil
    }
    if rowsToDelete > 5000 {
        rowsToDelete = 5000
    }

    // Удаляем самые старые записи по id
    delRes, err := lg.pool.Exec(ctx,
        fmt.Sprintf(
            "DELETE FROM %s WHERE id IN (SELECT id FROM %s ORDER BY id LIMIT %d)",
            lg.tableName, lg.tableName, rowsToDelete,
        ),
    )
    if err != nil {
        return fmt.Errorf("pre-VACUUM delete failed: %w", err)
    }
    deleted := delRes.RowsAffected()
    log.Printf("Deleted %d live rows before VACUUM FULL", deleted)
    return nil
}