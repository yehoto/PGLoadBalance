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
	pool           *pgxpool.Pool
	mon            *monitoring.Monitoring
	minSizeMB      int64
	maxSizeMB      int64
	mode           atomic.Int32
	workers        int
	numTables      int
	estimatedSize  atomic.Int64 // Оценка размера базы данных
}

// Средний размер строки (10 КБ данных + ~24 байта заголовка)
const rowSizeBytes int64 = 10*1024 + 24

func New(pool *pgxpool.Pool, mon *monitoring.Monitoring, minSize, maxSize int64, workers, numTables int) *LoadGen {
	lg := &LoadGen{
		pool:           pool,
		mon:            mon,
		minSizeMB:      minSize,
		maxSizeMB:      maxSize,
		workers:        workers,
		numTables:      numTables,
	}
	lg.mode.Store(int32(insertMode))
	return lg
}

func (lg *LoadGen) Run(ctx context.Context) {
	// Создание таблиц и отключение autovacuum
	for i := 1; i <= lg.numTables; i++ {
		table := fmt.Sprintf("test_%d", i)
		lg.pool.Exec(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id bigserial PRIMARY KEY, data text)", table))
		lg.pool.Exec(ctx, fmt.Sprintf("ALTER TABLE %s SET (autovacuum_enabled = off)", table))
	}

	// Привязка каждого потока к таблице
	var wg sync.WaitGroup
	for i := 0; i < lg.workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			tableID := (workerID % lg.numTables) + 1
			table := fmt.Sprintf("test_%d", tableID)
			lg.worker(ctx, table)
		}(i)
	}


	// Мониторинг размера и переключение режима
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			size, err := lg.mon.GetDBSize()
			if err != nil {
				fmt.Println("Ошибка мониторинга:", err)
				continue
			}
			fmt.Printf("Current DB size: %d MB\n", size)
			lg.estimatedSize.Store(size)
			if size <= lg.minSizeMB+20 {
				lg.mode.Store(int32(insertMode))
			} else if size >= lg.maxSizeMB-20 {
				lg.mode.Store(int32(deleteMode))
			} else {
				modes := []int32{int32(insertMode), int32(deleteMode), int32(updateMode)}
				lg.mode.Store(modes[rand.Intn(len(modes))])
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
			currentMode := modeEnum(lg.mode.Load())
			switch currentMode {
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
	sizeMB := lg.estimatedSize.Load()
	if sizeMB >= lg.maxSizeMB {
		return
	}

	freeBytes := (lg.maxSizeMB - sizeMB) * 1024 * 1024
	allowedRows := int(freeBytes / rowSizeBytes)
	if allowedRows == 0 {
		return
	}
	if allowedRows > 100 {
		allowedRows = 100
	}

	batch := &pgx.Batch{}
	for i := 0; i < allowedRows; i++ {
		batch.Queue(fmt.Sprintf("INSERT INTO %s(data) VALUES($1)", table), randString(int(rowSizeBytes)))
	}
	br := lg.pool.SendBatch(ctx, batch)
	_ = br.Close()
}

func (lg *LoadGen) randomDelete(ctx context.Context, table string) {
	sizeMB := lg.estimatedSize.Load()
	if sizeMB <= lg.minSizeMB {
		return
	}

	targetMB := (lg.minSizeMB + lg.maxSizeMB) / 2
	if sizeMB <= targetMB {
		return // Не удаляем, если размер уже ниже или равен целевому
	}

	removableBytes := (sizeMB - targetMB) * 1024 * 1024
	removableRows := int(removableBytes / rowSizeBytes)
	if removableRows < 1 {
		return
	}
	if removableRows > 5000 {
		removableRows = 5000
	}

	_, err := lg.pool.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE id IN (SELECT id FROM %s TABLESAMPLE SYSTEM (10) LIMIT %d)", table, table, removableRows))
	if err != nil {
		return
	}

		lg.checkAndVacuum(ctx, table)
	
}

func (lg *LoadGen) randomUpdate(ctx context.Context, table string) {
	sizeMB := lg.estimatedSize.Load()
	if sizeMB >= lg.maxSizeMB {
		return
	}

	freeBytes := (lg.maxSizeMB - sizeMB) * 1024 * 1024
	allowedRows := int(freeBytes / rowSizeBytes)
	if allowedRows == 0 {
		return
	}
	if allowedRows > 100 {
		allowedRows = 100
	}

	_, err := lg.pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET data = $1 WHERE id IN (SELECT id FROM %s TABLESAMPLE SYSTEM (1) LIMIT %d)", table, table, allowedRows), randString(int(rowSizeBytes)))
	if err != nil {
		return
	}

	
		lg.checkAndVacuum(ctx, table)
	
}

func (lg *LoadGen) checkAndVacuum(ctx context.Context, table string) {
	var dead int64
	err := lg.pool.QueryRow(ctx, "SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname=$1", table).Scan(&dead)
	if err != nil {
		return
	}
	const threshold = 3000 // Сниженный порог для более частого запуска VACUUM
	if dead > threshold {
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