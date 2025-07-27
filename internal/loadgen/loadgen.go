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
const (
    avgTupleLen = 1060
)



type LoadGen struct {
	pool             *pgxpool.Pool
	mon              *monitoring.Monitoring
	minSizeMB        int64
	maxSizeMB        int64
	tableName        string
	rand             *rand.Rand
	emptyTuplesBytes atomic.Int64 
	liveTupleBytes  atomic.Int64
	deadTupleBytes   atomic.Int64
    totalRelSize    atomic.Int64

	delta         atomic.Int64 // общий объём бд - ttotalRelSIze
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
				log.Printf("AAAAAAAAAAAA")
				
			}
            lg.totalRelSize.Store(totalSize)
			log.Printf("TOTAL: %d", totalSize)
	        lg.delta.Store(dbSizeMB*1024*1024-lg.totalRelSize.Load())
            lg.checkMaintenance(ctx)
			
			mode := insertMode
			
                if dbSizeMB >= lg.maxSizeMB {
                    mode = deleteMode
                } else if  dbSizeMB <= lg.minSizeMB {
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

	//freeBytes := lg.emptyTuplesBytes.Load()
	//avgLenTuples := lg.avgTupleLen.Load()
	//deadTupleBytes := lg.deadTupleBytes.Load()
    currentSize := lg.totalRelSize.Load() + lg.delta.Load()

	toDeleteBytesInt64 := currentSize - (lg.minSizeMB * 1024 * 1024)- lg.deadTupleBytes.Load() - lg.emptyTuplesBytes.Load()
    /*2025/07/24 05:41:29 TableSize=300498944B, LiveTupleBytes=103058113B, DeadCount=9, DeadBytes=9549B, FreeBytes=195631028B
2025/07/24 05:41:29 DB: 302 - зацикливание
     тут minSizeMB БОЛЬШЕ чем кол-во живых байт строк!!!!!!!!
     */
    toDeleteTuplesFloat := float64(toDeleteBytesInt64) / float64(avgTupleLen)
     toDeleteTuples := int(toDeleteTuplesFloat)

//       liveTupleBytes := lg.liveTupleBytes.Load() 
   
//    if toDeleteTuples >= int(float64(liveTupleBytes)/float64(avgTupleLen)) { // Convert liveTupleBytes to float64 after loading
//     toDeleteTuples = int(float64(liveTupleBytes) / float64(avgTupleLen)) // convert to int after calculation
//   }

    if toDeleteTuples <= 0 {
		toDeleteTuples = 0
	} else  if toDeleteTuples >= 5000 {
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
    rowsBySpace := freeBytes / int64 (avgTupleLen)
    rowsByLive := liveTupleCount
    maxRows := rowsBySpace
    if rowsByLive < maxRows {
        maxRows = rowsByLive
    }

    maxRows = maxRows + ((lg.liveTupleBytes.Load()/avgTupleLen)/2)
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
    if float64(rangeSize)*0.2 <= float64(freeBytes) {
        log.Printf("checkMaintenance: запуск предварительной очистки")
        
        // Вызов с ЛОГИРОВАНИЕМ
        if err := lg.preVacuumDelete(ctx); err != nil {
            log.Printf("preVacuumDelete error: %v", err)
        }

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
	        lg.delta.Store(dbSizeMB*1024*1024-lg.totalRelSize.Load())
      
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

// Измените сигнатуру функции, убрав tupleCount и deadCount
func (lg *LoadGen) getPgstattupleStats(ctx context.Context) (
    // tupleCount   int64,  // <-- Убрано
    tupleLenBytes   int64, // 1 (было 2)
    // deadCount    int64,  // <-- Убрано
    deadLenBytes    int64, // 2 (было 4)
    freeSpaceBytes  int64, // 3 (было 5)
    err             error, // 4 (было 6)
) {
    // ... (тело функции остается почти тем же)
    var tableLen    int64
     var deadCount int64 // <-- Локальная переменная, нужна ТОЛЬКО для Scan
    var localTupleCount int64 // <-- Эта локальная переменная все еще нужна для Scan
    var tuplePct    float64
    var deadPct     float64 // <-- Эта локальная переменная все еще нужна для Scan
    var freePct     float64

    query := fmt.Sprintf(`
        SELECT
            table_len,
            tuple_count,     -- Нужно для Scan, даже если не возвращаем
            tuple_len,
            tuple_percent,   -- Нужно для Scan, даже если не возвращаем
            dead_tuple_count, -- Нужно для Scan, даже если не возвращаем
            dead_tuple_len,
            dead_tuple_percent, -- Нужно для Scan, даже если не возвращаем
            free_space,
            free_percent      -- Нужно для Scan, даже если не возвращаем
        FROM pgstattuple('%s')`, lg.tableName)

    // Scan *все* поля, включая ненужные нам сейчас
    err = lg.pool.QueryRow(ctx, query).Scan(
        &tableLen,        // table_len
        &localTupleCount, // tuple_count (игнорируется после Scan)
        &tupleLenBytes,   // tuple_len (возвращается как 1-е значение)
        &tuplePct,        // tuple_percent (игнорируется)
        &deadCount,       // dead_tuple_count (игнорируется, но переменная нужна!)
        &deadLenBytes,    // dead_tuple_len (возвращается как 2-е значение)
        &deadPct,         // dead_tuple_percent (игнорируется)
        &freeSpaceBytes,  // free_space (возвращается как 3-е значение)
        &freePct,         // free_percent (игнорируется)
    )
    if err != nil {
        // Возврат нулей для оставшихся int64 и ошибки
        return 0, 0, 0, fmt.Errorf("pgstattuple scan failed: %w", err)
    }

    // Возврат только нужных значений
    // Порядок теперь: tupleLenBytes, deadLenBytes, freeSpaceBytes, error
    return tupleLenBytes, deadLenBytes, freeSpaceBytes, nil
}



// Добавляем метод для предварительного удаления живых строк
func (lg *LoadGen) preVacuumDelete(ctx context.Context) error {
    currentTableSize := lg.totalRelSize.Load()+lg.delta.Load()
    minSizeBytes := lg.minSizeMB * 1024 * 1024

    // Логирование текущих параметров
    log.Printf(
        "preVacuumDelete: current=%dMB, min=%dMB, tableSizeBytes=%d, minSizeBytes=%d",
        currentTableSize/(1024*1024),
        lg.minSizeMB,
        currentTableSize,
        minSizeBytes,
    )

    if lg.liveTupleBytes.Load() <= minSizeBytes {
        log.Printf("preVacuumDelete: таблица уже меньше минимального размера")
        return nil
    }

    bytesToRemove := currentTableSize - minSizeBytes - lg.deadTupleBytes.Load() - lg.emptyTuplesBytes.Load()

    
    // Логирование дополнительных параметров
    log.Printf(
        "preVacuumDelete: bytesToRemove=%d",
        bytesToRemove,
    )
   
    rowsToDelete := (bytesToRemove) / avgTupleLen

    log.Printf("preVacuumDelete: удаление %d строк...", rowsToDelete)

    res, err := lg.pool.Exec(ctx,
        fmt.Sprintf(
            "DELETE FROM %s WHERE id IN (SELECT id FROM %s ORDER BY id LIMIT %d)",
            lg.tableName, lg.tableName, rowsToDelete,
        ),
    )
    if err != nil {
        return fmt.Errorf("preVacuumDelete: ошибка удаления: %w", err)
    }

    deleted := res.RowsAffected()
    log.Printf("preVacuumDelete: удалено %d строк", deleted)
    return nil
}