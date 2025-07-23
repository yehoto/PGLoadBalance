package loadgen

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
    "math"
	"sync/atomic"
	"time"
	"bytes"
	"runtime"
	"strconv"

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
	minBatch    = 10
	maxBatch    = 5000
	mbPerRow    = 1 // 1 строка ≈ 1MB
)

// Добавляем тип для VACUUM
type vacuumType int

const (
	vacuumNormal vacuumType = iota
	vacuumFull
)

type LoadGen struct {
	pool             *pgxpool.Pool
	mon              *monitoring.Monitoring
	minSizeMB        int64
	maxSizeMB        int64
	tableName        string
	rand             *rand.Rand
	rwmu             sync.RWMutex // lock for VACUUM
	vacuumActive     bool         // VACUUM flag
	vacuumReq        chan vacuumType
	vacuumFullThreshold int64 // 80% от maxSizeMB
}

var (
    globalDeadTuples atomic.Int64
    globalFreeSpace  atomic.Int64
)

func New(pool *pgxpool.Pool, mon *monitoring.Monitoring, minSize, maxSize int64) *LoadGen {
	return &LoadGen{
		pool:      pool,
		mon:       mon,
		minSizeMB: minSize,
		maxSizeMB: maxSize,
		tableName: "test",
		rand:      rand.New(rand.NewSource(time.Now().UnixNano())),
		vacuumReq: make(chan vacuumType, 1),
		//vacuumFullThreshold: int64(float64(maxSize) * 0.8), // Рассчитываем порог
	}
}

func (lg *LoadGen) Run(ctx context.Context) {
	log.Printf("LoadGen started in goroutine ID: %d", getGoroutineID())
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

        lg.pool.Exec(ctx, fmt.Sprintf(
    `ALTER TABLE %s 
    SET (
        autovacuum_enabled = off,
        toast.autovacuum_enabled = off,
        vacuum_truncate = off  // ДОБАВЛЯЕМ ЭТУ СТРОЧКУ
    )`, lg.tableName))
       

	// Увеличиваем интервал для более плавной работы
    ticker := time.NewTicker(10 * time.Millisecond) // было 50 мс
    defer ticker.Stop()

    go lg.vacuumWorker(ctx)

    // Добавляем буфер для стабильности
    buffer := lg.maxSizeMB * 5 / 100 // 5% буфер
    lowThreshold := lg.minSizeMB + buffer
    highThreshold := lg.maxSizeMB - buffer

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            sz, err := lg.mon.GetDBSize()
            if err != nil {
                log.Printf("Failed to get DB size: %v", err)
                continue
            }
             log.Printf("Current DB size: %d MB", sz)

            var mode modeEnum

            if sz > highThreshold {
                mode = deleteMode
            } else if sz < lowThreshold {
                mode = insertMode
            } else {
                // В буферной зоне - случайный выбор
                mode = modeEnum(lg.rand.Intn(2))
            }

            switch mode {
            case insertMode:
                lg.safeOp(ctx, lg.batchInsert)
            case deleteMode:
                lg.safeOp(ctx, lg.randomDelete)
            }
        }
    }
}

func (lg *LoadGen) safeOp(ctx context.Context, op func(context.Context) error) {
	lg.rwmu.RLock()
	defer lg.rwmu.RUnlock()

	if lg.vacuumActive {
		log.Println("Operation waiting for VACUUM completion")
	}

	if err := op(ctx); err != nil {
		log.Printf("Operation failed: %v", err)
	}
}

func (lg *LoadGen) vacuumWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-lg.vacuumReq:
			switch t {
			case vacuumNormal:
				lg.performVacuum(ctx)
			case vacuumFull:
				lg.performVacuumFull(ctx)
			}
		}
	}
}

// Запрос обычного VACUUM
func (lg *LoadGen) requestVacuumNormal() {
	select {
	case lg.vacuumReq <- vacuumNormal:
		log.Println("Normal vacuum requested")
	default:
		// Уже есть запрос
	}
}

// Приоритетный запрос VACUUM FULL
func (lg *LoadGen) requestVacuumFull() {
	// Очищаем канал для приоритета FULL
	select {
	case <-lg.vacuumReq:
		log.Println("Overriding existing vacuum request with FULL")
	default:
	}
	select {
	case lg.vacuumReq <- vacuumFull:
		log.Println("Full vacuum requested")
	default:
	}
}

func (lg *LoadGen) updateStats(ctx context.Context) error {
    dead, free, err := lg.getPgstattupleStats(ctx)
    if err != nil {
        return fmt.Errorf("failed to get pgstattuple stats: %w", err)
    }
    globalDeadTuples.Store(dead)
    globalFreeSpace.Store(free)
    log.Printf("Stats updated: dead_tuples=%d, free_space=%d", dead, free)
    return nil
}


// func (lg *LoadGen) batchInsert(ctx context.Context) error {
//     currentSize, err := lg.mon.GetDBSize()
//     if err != nil {
//         return fmt.Errorf("failed to get DB size: %w", err)
//     }

//     // 1. Нормируем в [0,1]
//     span := float64(lg.maxSizeMB - lg.minSizeMB)
//     ratio := float64(currentSize - lg.minSizeMB) / span
//     if ratio < 0 {
//         ratio = 0
//     } else if ratio > 1 {
//         ratio = 1
//     }

//     // 2. Синусоидальное скейлирование: в начале и в конце — 0, в середине — 1
//     scale := math.Sin(math.Pi * ratio)

//     // 3. Определяем максимальное число строк за раз
//     const hardMaxRows = 5000
//     rowsToInsert := int(scale * hardMaxRows)
//     if rowsToInsert < 1 {
//         rowsToInsert = 1
//     }

//     batch := &pgx.Batch{}
//     for i := 0; i < rowsToInsert; i++ {
//         batch.Queue(
//             fmt.Sprintf("INSERT INTO %s(data) VALUES($1)", lg.tableName),
//             randString(1024, lg.rand),
//         )
//     }

//     if err := lg.pool.SendBatch(ctx, batch).Close(); err != nil {
//         return fmt.Errorf("batch insert failed: %w", err)
//     }

//     log.Printf("Inserted rows: %d (scale=%.2f)", rowsToInsert, scale)
//     lg.checkMaintenance(ctx, lg.tableName)
//     return nil
//}
// batchInsert: вставляем пропорционально тому, сколько ещё МБ можно добавить
func (lg *LoadGen) batchInsert(ctx context.Context) error {
    // 1) получаем текущий размер и статистику
    currentSize, err := lg.mon.GetDBSize()
    if err != nil {
        return err
    }
    _, freeBytes, err := lg.getPgstattupleStats(ctx)
    if err != nil {
        return err
    }
    freeMB := freeBytes / (1024 * 1024)

    // 2) сколько уже занято, и сколько ещё можно вставить до maxSizeMB
    usedMB := currentSize - freeMB
    toMaxMB := lg.maxSizeMB - usedMB

    // 3) считаем rows: пропорционально toMaxMB, но в пределах [minBatch, maxBatch]
    rowsF := math.Min(math.Max(float64(toMaxMB), float64(minBatch)), float64(maxBatch))
    rows := int(rowsF)

    // 4) формируем batch вставок
    batch := &pgx.Batch{}
    for i := 0; i < rows; i++ {
        batch.Queue(
            fmt.Sprintf("INSERT INTO %s(data) VALUES($1)", lg.tableName),
            randString(1024, lg.rand),
        )
    }
    if err := lg.pool.SendBatch(ctx, batch).Close(); err != nil {
        return err
    }

    log.Printf("Inserted rows=%d (used=%.0fMB, free=%.0fMB)", rows, usedMB, freeMB)
    lg.checkMaintenance(ctx, lg.tableName)
    return nil
}





// func (lg *LoadGen) randomDelete(ctx context.Context) error {
// 	const remRows = 5000
// 	res, err := lg.pool.Exec(ctx,
// 		fmt.Sprintf("DELETE FROM %s WHERE id IN (SELECT id FROM %s ORDER BY id LIMIT %d)",
// 			lg.tableName, lg.tableName, remRows),
// 	)
// 	if err != nil {
// 		return fmt.Errorf("delete failed: %w", err)
// 	}
// 	affected := res.RowsAffected()
// 	lg.manualDeadTuples.Add(affected)
// 	sz, _ := lg.mon.GetDBSize()
// 	log.Printf("Post-delete DB size: %d MB", sz)
// 	log.Printf("EMPTY_TUP: %d", lg.manualEmptyTuples.Load())
// 	lg.checkMaintenance(ctx, lg.tableName)
// 	return nil
// }
// randomDelete плавно масштабирует количество удаляемых строк синусоидой,
// резче у верхней границы и пологее в середине.
// randomDelete учитывает currentSize и free_space
// randomDelete учитывает currentSize и free_space
// randomDelete: удаляем пропорционально тому, насколько мы выше minSizeMB
func (lg *LoadGen) randomDelete(ctx context.Context) error {
    // 1) получаем текущий размер и статистику
    currentSize, err := lg.mon.GetDBSize()
    if err != nil {
        return err
    }
    _, freeBytes, err := lg.getPgstattupleStats(ctx)
    if err != nil {
        return err
    }
    freeMB := freeBytes / (1024 * 1024)

    // 2) сколько занято и насколько мы выше minSizeMB
    usedMB := currentSize - freeMB
    overMinMB := usedMB - lg.minSizeMB
    if overMinMB < 1 {
        overMinMB = 1
    }

    // 3) считаем rows: пропорционально overMinMB, но в пределах [minBatch, maxBatch]
    rowsF := math.Min(math.Max(float64(overMinMB), float64(minBatch)), float64(maxBatch))
    rows := int(rowsF)

    // 4) выполняем DELETE
    q := fmt.Sprintf(`DELETE FROM %s WHERE ctid IN (
        SELECT ctid FROM %s WHERE xmax = 0 LIMIT %d
    )`, lg.tableName, lg.tableName, rows)
    res, err := lg.pool.Exec(ctx, q)
    if err != nil {
        return err
    }

    log.Printf("Deleted rows=%d (overMin=%.0fMB, used=%.0fMB)", res.RowsAffected(), overMinMB, usedMB)
    lg.checkMaintenance(ctx, lg.tableName)
    return nil
}

// func (lg *LoadGen) randomUpdate(ctx context.Context) error {
// 	const maxRows = 2000
// 		// include no_hot toggle to prevent HOT
// 	res , err := lg.pool.Exec(ctx,
// 		fmt.Sprintf(
// 			"UPDATE %s SET data = $1, no_hot = NOT no_hot WHERE id IN (SELECT id FROM %s ORDER BY id LIMIT %d)",
// 			lg.tableName, lg.tableName, maxRows,
// 		),
// 		randString(1024, lg.rand),
// 	)
// 	if err != nil {
// 		return fmt.Errorf("update failed: %w", err)
// 	}
// 	// Обработка manualEmptyTuples для UPDATE
// 	affected := res.RowsAffected()
// 	currentEmpty := lg.manualEmptyTuples.Load()
// 	if currentEmpty > 0 {
// 		subValue := affected
// 		if currentEmpty < subValue {
// 			subValue = currentEmpty
// 		}
// 		lg.manualEmptyTuples.Add(-subValue)
// 	}

// 	lg.manualDeadTuples.Add(affected)
// 	sz, _ := lg.mon.GetDBSize()
// 	log.Printf("Post-update DB size: %d MB", sz)
// 	log.Printf("EMPTY_TUP: %d", lg.manualEmptyTuples.Load())
// 	lg.checkMaintenance(ctx, lg.tableName)
// 	return nil
// }

func (lg *LoadGen) randomUpdate(ctx context.Context) error {
    // Обновление данных
    res, err := lg.pool.Exec(ctx,
        fmt.Sprintf(
            "UPDATE %s SET data = $1, no_hot = NOT no_hot WHERE id IN (SELECT id FROM %s ORDER BY id LIMIT %d)",
            lg.tableName, lg.tableName, 2000,
        ),
        randString(1024, lg.rand),
    )
    if err != nil {
        return fmt.Errorf("update failed: %w", err)
    }

    affected := res.RowsAffected()
    log.Printf("Updated rows: %d", affected)

    // Обновляем глобальные переменные через getPgstattupleStats
    // if err := lg.updateStats(ctx); err != nil {
    //     log.Printf("Failed to update stats after update: %v", err)
    // }

    // Проверка необходимости VACUUM
    lg.checkMaintenance(ctx, lg.tableName)

    return nil
}

func (lg *LoadGen) getDeadViaPgstattuple(ctx context.Context) (int64, error) {
	var deadCount int64
	query := fmt.Sprintf(
		"SELECT dead_tuple_count FROM pgstattuple('%s')",
		lg.tableName,
	)
	err := lg.pool.QueryRow(ctx, query).Scan(&deadCount)
	return deadCount, err
}

func (lg *LoadGen) checkMaintenance(ctx context.Context, table string) {
    dead, free, _ := lg.getPgstattupleStats(ctx)
    sz, _         := lg.mon.GetDBSize()
    freeMB        := free / (1024 * 1024)
    usedMB        := sz - freeMB

    // Норма «мертвых» растёт по мере приближения к maxSize
    ratio := float64(usedMB - lg.minSizeMB) / float64(lg.maxSizeMB - lg.minSizeMB)
    dynamicDead := int64(100 + 900*ratio) // от 100 до 1000

    if dead > dynamicDead {
        lg.requestVacuumNormal()
    }
    lg.maybeFull(ctx)
}


const fullMargin = 10 // MB

func (lg *LoadGen) maybeFull(ctx context.Context) {
    sz, _     := lg.mon.GetDBSize()
    _, free, _:= lg.getPgstattupleStats(ctx)
    freeMB     := free/(1024*1024)
    usedMB     := sz - freeMB

    // Запускаем full только если мы далеко от minSize
    if usedMB > lg.minSizeMB + fullMargin {
        lg.requestVacuumFull()
    }
}



// func (lg *LoadGen) performVacuum(ctx context.Context) {
// 	lg.rwmu.Lock()
// 	lg.vacuumActive = true
// 	defer func() { lg.vacuumActive = false }()
// 	defer lg.rwmu.Unlock()

// 	log.Println("Starting VACUUM")
	
	
// 	_, err := lg.pool.Exec(ctx, fmt.Sprintf("VACUUM %s", lg.tableName))
// 	if err != nil {
// 		log.Printf("VACUUM failed: %v", err)
// 	}


// 	dead, err := lg.getDeadViaPgstattuple(ctx)
// 	if err != nil {
// 		log.Printf("Post-VACUUM pgstattuple failed: %v", err)
// 	} else {
// 		log.Printf("Post-VACUUM dead tuples=%d", dead)
		
// 	}
// 	 if err := lg.updateStats(ctx); err != nil {
//         log.Printf("Failed to update stats after delete: %v", err)
//     }
// 	// Сброс счетчика после VACUUM
    
// }

func (lg *LoadGen) performVacuum(ctx context.Context) {
    lg.rwmu.Lock()
    lg.vacuumActive = true
    defer func() { lg.vacuumActive = false }()
    defer lg.rwmu.Unlock()

    log.Println("Starting VACUUM")
    conn, err := lg.pool.Acquire(ctx)
    if err != nil {
        log.Printf("Failed to acquire connection: %v", err)
        return
    }
    defer conn.Release()

    // Запускаем VACUUM в отдельной горутине
    done := make(chan error, 1)
    go func() {
        _, err := conn.Exec(ctx, fmt.Sprintf(
        //"VACUUM (TRUNCATE false, DISABLE_PAGE_SKIPPING true, INDEX_CLEANUP ON, PROCESS_TOAST true) %s",
         "VACUUM (TRUNCATE false, DISABLE_PAGE_SKIPPING true, INDEX_CLEANUP ON, PROCESS_TOAST true) %s",
        lg.tableName,
    ))
        done <- err
    }()

    // Проверяем прогресс каждые 500мс
    ticker := time.NewTicker(500 * time.Millisecond)
    defer ticker.Stop()
    
    const vacuumTimeout = 30 * time.Minute
    timeout := time.After(vacuumTimeout)
    
    for {
        select {
        case err := <-done:
            if err != nil {
                log.Printf("VACUUM failed: %v", err)
            }
            log.Println("VACUUM successfully finished")
            return
        case <-ticker.C:
            var pid int
            query := `SELECT pid FROM pg_stat_progress_vacuum WHERE relid = $1::regclass`
            err := conn.QueryRow(ctx, query, lg.tableName).Scan(&pid)
            if err == pgx.ErrNoRows {
                log.Println("VACUUM process not found (likely finished)")
                continue
            } else if err != nil {
                log.Printf("Progress check failed: %v", err)
            } else {
                log.Printf("VACUUM in progress (PID: %d)", pid)
            }
        case <-timeout:
            log.Println("VACUUM timeout reached")
            return
        case <-ctx.Done():
            log.Println("Context canceled during VACUUM")
            return
        }
    }
}

func (lg *LoadGen) performVacuumFull(ctx context.Context) {
	//Проверяем актуальность размера перед выполнением
	sz, err := lg.mon.GetDBSize()
	if err != nil {
		log.Printf("Failed to get DB size: %v", err)
		return
	}
	if sz < lg.vacuumFullThreshold {
		log.Println("Skipping VACUUM FULL: DB size below threshold")
		//return
	}

	lg.rwmu.Lock()
	lg.vacuumActive = true
	defer func() { lg.vacuumActive = false }()
	defer lg.rwmu.Unlock()

	log.Println("Starting VACUUM FULL")
	_, err = lg.pool.Exec(ctx, fmt.Sprintf("VACUUM FULL %s", lg.tableName))
	if err != nil {
		log.Printf("VACUUM FULL failed: %v", err)
	}

	// Обновляем статистику после FULL
	_, err = lg.pool.Exec(ctx, fmt.Sprintf("ANALYZE %s", lg.tableName))
	if err != nil {
		log.Printf("ANALYZE failed: %v", err)
	}

	szAfter, _ := lg.mon.GetDBSize()
	log.Printf("DB size after VACUUM FULL: %d MB", szAfter)
	// Сброс счетчика после VACUUM
	 if err := lg.updateStats(ctx); err != nil {
        log.Printf("Failed to update stats after delete: %v", err)
    }
    
}

// func randString(n int, r *rand.Rand) string {
// 	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
// 	b := make([]byte, n)
// 	for i := range b {
// 		b[i] = letters[r.Intn(len(letters))]
// 	}
// 	return string(b)
// }

// Пример randString для completeness
func randString(n int, r *rand.Rand) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[r.Intn(len(letters))]
	}
	return string(b)
}

func getGoroutineID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func (lg *LoadGen) getPgstattupleStats(ctx context.Context) (deadTuples, freeSpace int64, err error) {
    query := fmt.Sprintf("SELECT dead_tuple_count, free_space FROM pgstattuple('%s')", lg.tableName)
    err = lg.pool.QueryRow(ctx, query).Scan(&deadTuples, &freeSpace)
    if err != nil {
        return 0, 0, err
    }
    return deadTuples, freeSpace, nil
}