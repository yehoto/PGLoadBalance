package loadgen

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"PGLoadBalance/internal/monitoring"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type modeEnum int32

const (
	insertMode modeEnum = iota
	deleteMode
	updateMode
)

type LoadGen struct {
	pool          *pgxpool.Pool
	mon           *monitoring.Monitoring
	minSizeMB     int64
	maxSizeMB     int64
	mode          atomic.Int32
	workers       int
	numTables     int
	estimatedSize atomic.Int64 // точная оценка размера базы
	deleting      atomic.Bool

}

// приблизительный размер строки (10 КБ + заголовок)
const rowSizeBytes int64 = 10*1024 + 24

// New создаёт генератор с фиксированным гистерезисом 20%
func New(pool *pgxpool.Pool, mon *monitoring.Monitoring, minSize, maxSize int64, workers, numTables int) *LoadGen {
	lg := &LoadGen{
		pool:      pool,
		mon:       mon,
		minSizeMB: minSize,
		maxSizeMB: maxSize,
		workers:   workers,
		numTables: numTables,
	}
	lg.mode.Store(int32(insertMode))
	return lg
}

func (lg *LoadGen) Run(ctx context.Context) {
	// Создать таблицы и включить autovacuum
	for i := 1; i <= lg.numTables; i++ {
		tbl := fmt.Sprintf("test_%d", i)
		lg.pool.Exec(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id bigserial PRIMARY KEY, data text)", tbl))
		lg.pool.Exec(ctx, fmt.Sprintf(`
            ALTER TABLE %s 
            SET (
                autovacuum_enabled = off,
                toast.autovacuum_enabled = off
            )`, tbl))
	}

	// Запустить воркеры
	var wg sync.WaitGroup
	for i := 0; i < lg.workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tblID := (id % lg.numTables) + 1
			tbl := fmt.Sprintf("test_%d", tblID)
			lg.worker(ctx, tbl)
		}(i)
	}

	// Мониторинг каждые 100ms для точной оценки и переключения режимов с гистерезисом 20%
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sz, err := lg.mon.GetDBSize()
			if err != nil {
				fmt.Println("Monitor error:", err)
				continue
			}
			fmt.Printf("Current DB size: %d MB\n", sz)
			lg.estimatedSize.Store(sz)

			// гистерезис 20%
			rangeMB := lg.maxSizeMB - lg.minSizeMB
			hysteresis := rangeMB / 5
			low := lg.minSizeMB + hysteresis
			high := lg.maxSizeMB - hysteresis

			switch {
			case sz < low:
				lg.mode.Store(int32(insertMode))
			case sz > high:
				lg.mode.Store(int32(deleteMode))
			default:
				// внутри гистерезиса — процентное выпадение режимов
				p := rand.Intn(100)
				switch {
				case p < 50:
					lg.mode.Store(int32(updateMode))
				case p < 75:
					lg.mode.Store(int32(insertMode))
				default:
					lg.mode.Store(int32(deleteMode))
				}
			}
		}
	}
}

func (lg *LoadGen) worker(ctx context.Context, table string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			switch modeEnum(lg.mode.Load()) {
			case insertMode:
				lg.batchInsert(ctx, table)
			case deleteMode:
				lg.randomDelete(ctx, table)
			case updateMode:
				lg.randomUpdate(ctx, table)
			}
		}
	}
}

func (lg *LoadGen) batchInsert(ctx context.Context, table string) {
	sz := lg.estimatedSize.Load()
	if sz >= lg.maxSizeMB {
		return
	}

	freeB := (lg.maxSizeMB - sz) * 1024 * 1024
	maxRows := int(freeB / rowSizeBytes)
	if maxRows <= 0 {
		return
	}
	if maxRows > 20 {
		maxRows = 20
	}

	batch := &pgx.Batch{}
	for i := 0; i < maxRows; i++ {
		batch.Queue(fmt.Sprintf("INSERT INTO %s(data) VALUES($1)", table), randString(int(rowSizeBytes)))
	}
	br := lg.pool.SendBatch(ctx, batch)
	_ = br.Close()
}

func (lg *LoadGen) randomDelete(ctx context.Context, table string) {
	sz := lg.estimatedSize.Load()
	if sz <= lg.minSizeMB + 40 {
	return
   }
   if !lg.deleting.CompareAndSwap(false, true) {
    return // уже удаляется другим потоком
   }
    defer lg.deleting.Store(false)




	target := lg.minSizeMB + (lg.maxSizeMB - lg.minSizeMB)/4
	if sz <= target {
		return
	}

	remB := (sz - target) * 1024 * 1024
	remRows := int(remB / rowSizeBytes)
	if remRows < 1 {
		return
	}
	if remRows > 100 {
		remRows = 100
	}

	_, err := lg.pool.Exec(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE id IN (SELECT id FROM %s TABLESAMPLE SYSTEM (5) LIMIT %d)", table, table, remRows))
	if err == nil {
		lg.checkMaintenance(ctx, table)
	}
}

func (lg *LoadGen) randomUpdate(ctx context.Context, table string) {
	sz := lg.estimatedSize.Load()
	if sz >= lg.maxSizeMB {
		return
	}

	freeB := (lg.maxSizeMB - sz) * 1024 * 1024
	maxRows := int(freeB / rowSizeBytes)
	if maxRows <= 0 {
		return
	}
	if maxRows > 20 {
		maxRows = 20
	}

	_, err := lg.pool.Exec(ctx,
		fmt.Sprintf("UPDATE %s SET data = $1 WHERE id IN (SELECT id FROM %s TABLESAMPLE SYSTEM (1) LIMIT %d)", table, table, maxRows),
		randString(int(rowSizeBytes)))
	if err == nil {
		lg.checkMaintenance(ctx, table)
	}
}

func (lg *LoadGen) checkMaintenance(ctx context.Context, table string) {
	var dead int64
	err := lg.pool.QueryRow(ctx,
		"SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname=$1", table).Scan(&dead)
	if err != nil {
		return
	}

	if dead > 300 {
		lg.pool.Exec(ctx, fmt.Sprintf("VACUUM %s", table))
	}

}

func randString(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
