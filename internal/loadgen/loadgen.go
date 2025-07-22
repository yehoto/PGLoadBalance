package loadgen

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
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
	manualDeadTuples atomic.Int64 // manual dead count
	manualEmptyTuples atomic.Int64 // manual dead count
}

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

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	go lg.vacuumWorker(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sz , err := lg.mon.GetDBSize()
			if err != nil {
				log.Printf("Failed to get DB size: %v", err)
				continue
			}

			// Проверка порога для VACUUM FULL
			//if sz >= lg.vacuumFullThreshold {
				//lg.requestVacuumFull()
			//}

		// 	mode := modeEnum(lg.rand.Intn(3))

		// switch mode {
		// case insertMode:
		// 	lg.safeOp(ctx, lg.batchInsert)
		// case deleteMode:
		// 	lg.safeOp(ctx, lg.randomDelete)
		// case updateMode:
		// 	lg.safeOp(ctx, lg.randomUpdate)
		// }

		mode := insertMode
		if sz > lg.maxSizeMB {
			mode = deleteMode
		} else if sz < lg.minSizeMB{
			mode = insertMode
		}else{
			mode = modeEnum(lg.rand.Intn(2)) // insert или delete
		}
			
		

		//mode := modeEnum(lg.rand.Intn(2))

		switch mode {
		case insertMode:
			lg.safeOp(ctx, lg.batchInsert)
		case deleteMode:
			lg.safeOp(ctx, lg.randomDelete)
		//case updateMode:
			//lg.safeOp(ctx, lg.randomUpdate)
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

// func (lg *LoadGen) batchInsert(ctx context.Context) error {
// 	const maxRows = 1000
// 	batch := &pgx.Batch{}
// 	for i := 0; i < maxRows; i++ {
// 		batch.Queue(
// 			fmt.Sprintf("INSERT INTO %s(data) VALUES($1)", lg.tableName),
// 			randString(1024, lg.rand),
// 		)
// 	}

// 	if err := lg.pool.SendBatch(ctx, batch).Close(); err != nil {
// 		return fmt.Errorf("batch insert failed: %w", err)
// 	}

// 	// Обработка manualEmptyTuples
// 	currentEmpty := lg.manualEmptyTuples.Load()
// 	if currentEmpty > 0 {
// 		subValue := int64(maxRows)
// 		if currentEmpty < subValue {
// 			subValue = currentEmpty
// 		}
// 		lg.manualEmptyTuples.Add(-subValue)
// 	}

// 	sz, _ := lg.mon.GetDBSize()
// 	log.Printf("Post-insert DB size: %d MB", sz)
// 	log.Printf("EMPTY_TUP: %d", lg.manualEmptyTuples.Load())
// 	return nil
// }
func (lg *LoadGen) batchInsert(ctx context.Context) error {
	currentSize, err := lg.mon.GetDBSize()
	if err != nil {
		return fmt.Errorf("failed to get DB size: %w", err)
	}

	empty := lg.manualEmptyTuples.Load()
	freeSpace := lg.maxSizeMB - currentSize + empty
	if freeSpace < 0 {
		freeSpace = 0
	}

	maxInsertRows := int(freeSpace)
	if maxInsertRows == 0 {
		log.Println("No space available for insert")
		return nil
	}

	rowsToInsert := lg.rand.Intn(maxInsertRows + 1)
	if rowsToInsert == 0 {
		return nil // не вставляем ноль строк
	}

	batch := &pgx.Batch{}
	for i := 0; i < rowsToInsert; i++ {
		batch.Queue(
			fmt.Sprintf("INSERT INTO %s(data) VALUES($1)", lg.tableName),
			randString(1024, lg.rand),
		)
	}

	if err := lg.pool.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("batch insert failed: %w", err)
	}

	// Обновляем empty tuple счётчик
	currentEmpty := lg.manualEmptyTuples.Load()
	subValue := int64(rowsToInsert)
	if currentEmpty < subValue {
		subValue = currentEmpty
	}
	lg.manualEmptyTuples.Add(-subValue)

	log.Printf("Inserted rows: %d", rowsToInsert)
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
func (lg *LoadGen) randomDelete(ctx context.Context) error {
	currentSize, err := lg.mon.GetDBSize()
	if err != nil {
		return fmt.Errorf("failed to get DB size: %w", err)
	}

	//empty := lg.manualEmptyTuples.Load()
	//dead := lg.manualDeadTuples.Load()
	//availableToDelete := currentSize - lg.minSizeMB - empty - dead
	availableToDelete := currentSize - lg.minSizeMB
	if availableToDelete < 0 {
		availableToDelete = 0
	}

	rowsToDelete := lg.rand.Intn(int(availableToDelete) + 1)
	if rowsToDelete == 0 {
		return nil // ничего не удаляем
	}

	res, err := lg.pool.Exec(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE id IN (SELECT id FROM %s ORDER BY id LIMIT %d)",
			lg.tableName, lg.tableName, rowsToDelete),
	)
	if err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}

	affected := res.RowsAffected()
	lg.manualDeadTuples.Add(affected)

	log.Printf("Deleted rows: %d", affected)
	lg.checkMaintenance(ctx, lg.tableName)
	return nil
}


func (lg *LoadGen) randomUpdate(ctx context.Context) error {
	const maxRows = 2000
		// include no_hot toggle to prevent HOT
	res , err := lg.pool.Exec(ctx,
		fmt.Sprintf(
			"UPDATE %s SET data = $1, no_hot = NOT no_hot WHERE id IN (SELECT id FROM %s ORDER BY id LIMIT %d)",
			lg.tableName, lg.tableName, maxRows,
		),
		randString(1024, lg.rand),
	)
	if err != nil {
		return fmt.Errorf("update failed: %w", err)
	}
	// Обработка manualEmptyTuples для UPDATE
	affected := res.RowsAffected()
	currentEmpty := lg.manualEmptyTuples.Load()
	if currentEmpty > 0 {
		subValue := affected
		if currentEmpty < subValue {
			subValue = currentEmpty
		}
		lg.manualEmptyTuples.Add(-subValue)
	}

	lg.manualDeadTuples.Add(affected)
	sz, _ := lg.mon.GetDBSize()
	log.Printf("Post-update DB size: %d MB", sz)
	log.Printf("EMPTY_TUP: %d", lg.manualEmptyTuples.Load())
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
	_, err := lg.pool.Exec(ctx, fmt.Sprintf("ANALYZE %s", table))
	if err != nil {
		log.Printf("ANALYZE failed: %v", err)
		return
	}

	dead, err := lg.getDeadViaPgstattuple(ctx)
	if err != nil {
		log.Printf("pgstattuple failed: %v", err)
		return
	}

	log.Printf("Dead tuples (accurate)=%d, manual=%d", dead, lg.manualDeadTuples.Load())
	log.Printf("SRAVN EMPTY_TUP: %d", lg.manualEmptyTuples.Load())

	if dead > 1000 {
		log.Printf("Dead tuples threshold exceeded: %d", dead)
		lg.requestVacuumNormal()
	}
}

func (lg *LoadGen) performVacuum(ctx context.Context) {
	lg.rwmu.Lock()
	lg.vacuumActive = true
	defer func() { lg.vacuumActive = false }()
	defer lg.rwmu.Unlock()

	log.Println("Starting VACUUM")
	// Упрощенная логика: добавляем все dead tuples в empty tuples
	cleaned := lg.manualDeadTuples.Load()
	if cleaned > 0 {
		lg.manualEmptyTuples.Add(cleaned)
	}
	log.Printf("EMPTY_TUP: %d", lg.manualEmptyTuples.Load())
	_, err := lg.pool.Exec(ctx, fmt.Sprintf("VACUUM %s", lg.tableName))
	if err != nil {
		log.Printf("VACUUM failed: %v", err)
	}


	dead, err := lg.getDeadViaPgstattuple(ctx)
	if err != nil {
		log.Printf("Post-VACUUM pgstattuple failed: %v", err)
	} else {
		log.Printf("Post-VACUUM dead tuples=%d", dead)
		log.Printf("ost-VACUUM EMPTY_TUP: %d", lg.manualEmptyTuples.Load())
	}
	// Сброс счетчика после VACUUM
    lg.manualDeadTuples.Store(0)
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
    lg.manualEmptyTuples.Store(0)
	lg.manualDeadTuples.Store(0)
}

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
    var relPages int64
    var relTuples float64
    query := fmt.Sprintf("SELECT * FROM pgstattuple('%s')", lg.tableName)
    err = lg.pool.QueryRow(ctx, query).Scan(&relPages, &relTuples, &deadTuples, &freeSpace)
    if err != nil {
        return 0, 0, err
    }
    return deadTuples, freeSpace, nil
}