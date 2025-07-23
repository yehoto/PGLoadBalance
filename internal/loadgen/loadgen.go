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

            sz := lg.tableLenBytes.Load() / (1024 * 1024) // Конвертируем байты в мегабайты

			mode := insertMode
			// В основном цикле обработки
                if sz > lg.maxSizeMB || szy > lg.maxSizeMB {
                    mode = deleteMode
                } else if sz < lg.minSizeMB || szy < lg.minSizeMB {
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
    lg.checkMaintenance(ctx)
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
    lg.checkMaintenance(ctx)
	freeBits := lg.countEmptyTuples.Load()
	//avgLenTuples := lg.avgTupleLen.Load()
	deadTupleBytes := lg.deadTupleBytes.Load()
    currentSize := lg.tableLenBytes.Load()

    var avgLenTuples int64
    avgLenTuples = 1024

	availableToDeleteBytesInt64 := currentSize - (lg.minSizeMB * 1024 * 1024) - deadTupleBytes - freeBits
    availableToDeleteBytesFloat := float64(availableToDeleteBytesInt64) / float64(avgLenTuples)
     availableToDeleteBytes := int(availableToDeleteBytesFloat)

	if availableToDeleteBytes < 0 {
		availableToDeleteBytes = 0
	}
    if availableToDeleteBytes > 5000 {
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
    lg.checkMaintenance(ctx)
    
    // Получаем текущую статистику
    freeBytes := lg.countEmptyTuples.Load()
    avgTupleSize := lg.avgTupleLen.Load()
    if avgTupleSize == 0 {
        avgTupleSize = 1024 // консервативная оценка
    }
    
    // Получаем количество живых строк
    var liveTupleCount int64
    err := lg.pool.QueryRow(ctx, 
        fmt.Sprintf("SELECT count(*) FROM %s", lg.tableName)).Scan(&liveTupleCount)
    if err != nil {
        return fmt.Errorf("failed to get live tuple count: %w", err)
    }
    
    // Рассчитываем максимальное количество строк для обновления
    rowsBySpace := freeBytes / avgTupleSize
    rowsByLive := liveTupleCount
    
    // Берем минимальное из двух значений
    maxRows := rowsBySpace
    if rowsByLive < maxRows {
        maxRows = rowsByLive
    }
    
    // Если нет доступных строк или места - выходим
    if maxRows <= 0 {
        return nil
    }
    
    // Ограничиваем размер пакета
    if maxRows > 5000 {
        maxRows = 5000
    }
    
    // Выбираем случайное количество строк для обновления
    rowsToUpdate := int(maxRows)
    if rowsToUpdate > 1 {
        rowsToUpdate = lg.rand.Intn(rowsToUpdate) + 1
    }
    
    // Выполняем обновление
    res, err := lg.pool.Exec(ctx,
        fmt.Sprintf(`
            UPDATE %s 
            SET data = $1, no_hot = true 
            WHERE id IN (
                SELECT id FROM %s 
                ORDER BY random() 
                LIMIT %d
            )`,
            lg.tableName, lg.tableName, rowsToUpdate),
        randString(1024, lg.rand), // новые данные
    )
    if err != nil {
        return fmt.Errorf("update failed: %w", err)
    }
    
    affected := res.RowsAffected()
    log.Printf("Updated rows: %d", affected)
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

	if deadCount > 1000 {
		log.Printf("Trigger manual VACUUM: dead tuples=%d", deadCount)
		if _, err := lg.pool.Exec(ctx, fmt.Sprintf("VACUUM %s", lg.tableName)); err != nil {
			log.Printf("VACUUM failed: %v", err)
		}
	}

     // Сохраняем метрики
    currentSize := lg.tableLenBytes.Load()
    reclaimable := deadBytes + freeBytes
    rangeSize := (lg.maxSizeMB - lg.minSizeMB) * 1024 * 1024
    minSizeBytes := lg.minSizeMB * 1024 * 1024

     // Условие 1: reclaimable > 20% диапазона (max-min)
     if float64(reclaimable) > 0.2*float64(rangeSize){
        // Проверяем, не опустимся ли ниже минимума после очистки
        if currentSize - reclaimable >= minSizeBytes {

             log.Printf("Trigger VACUUM FULL: reclaimable=%dMB (%.1f%% of range)",
                reclaimable/(1024*1024), 100*float64(reclaimable)/float64(rangeSize))
            
            if _, err := lg.pool.Exec(ctx, fmt.Sprintf("VACUUM FULL %s", lg.tableName)); err != nil {
                log.Printf("VACUUM FULL failed: %v", err)
            }
        } else {
            log.Printf("Trigger VACUUM + refill: reclaimable=%dMB, but after FULL would be below min",
                reclaimable/(1024*1024))
                // 1. Сначала обычный VACUUM
            if _, err := lg.pool.Exec(ctx, fmt.Sprintf("VACUUM %s", lg.tableName)); err != nil {
                log.Printf("VACUUM failed: %v", err)
            }
            // 2. Рассчитываем сколько нужно вставить до безопасного уровня
            safeThreshold := minSizeBytes + reclaimable
            bytesNeeded := safeThreshold - currentSize
             // 3. Вставляем данные порциями
            for bytesNeeded > 0 {
                select {
                case <-ctx.Done():
                    return
                default:
                    inserted, err := lg.insertBatchUntilSafe(ctx, bytesNeeded)
                    if err != nil {
                        log.Printf("refill insert failed: %v", err)
                        break
                    }
                    bytesNeeded -= inserted
                    
                    // Обновляем статистику после вставки
                    if _, _, _, freeBytes, err = lg.getPgstattupleStats(ctx); err == nil {
                        currentSize = lg.tableLenBytes.Load()
                        reclaimable = deadBytes + freeBytes
                    }
                }
            }
            
            // 4. Теперь можно безопасно выполнить VACUUM FULL
            log.Printf("Trigger VACUUM FULL after refill")
            if _, err := lg.pool.Exec(ctx, fmt.Sprintf("VACUUM FULL %s", lg.tableName)); err != nil {
                log.Printf("VACUUM FULL failed: %v", err)
            }
        }
    

        }



     }

     func (lg *LoadGen) insertBatchUntilSafe(ctx context.Context, bytesNeeded int64) (int64, error) {
    // Рассчитываем размер пакета
    avgSize := lg.avgTupleLen.Load()
    if avgSize == 0 {
        avgSize = 1024
    }
    
    rowsToInsert := int(bytesNeeded / avgSize)
    if rowsToInsert == 0 {
        rowsToInsert = 1
    }
    if rowsToInsert > 500 {
        rowsToInsert = 500
    }
    
    // Выполняем вставку
    batch := &pgx.Batch{}
    for i := 0; i < rowsToInsert; i++ {
        batch.Queue(
            fmt.Sprintf("INSERT INTO %s(data) VALUES($1)", lg.tableName),
            randString(1024, lg.rand),
        )
    }
    
    if err := lg.pool.SendBatch(ctx, batch).Close(); err != nil {
        return 0, fmt.Errorf("refill insert failed: %w", err)
    }
    
    insertedBytes := int64(rowsToInsert) * avgSize
    log.Printf("Refill insert: %d rows (~%dMB)", rowsToInsert, insertedBytes/(1024*1024))
    return insertedBytes, nil
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