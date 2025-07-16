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
	pool      *pgxpool.Pool
	mon       *monitoring.Monitoring
	minSizeMB int64
	maxSizeMB int64
	mode      atomic.Int32 // текущий режим работы
	workers   int
	sizeMu    sync.Mutex // чтобы избежать гонок при расчёте объёма
}

// средний размер одной строки в байтах (10 KB) – используется для точного контроля объёма
const rowSizeBytes int64 = 10 * 1024

func New(pool *pgxpool.Pool, mon *monitoring.Monitoring, minSize, maxSize int64, workers int) *LoadGen {
	lg := &LoadGen{pool: pool, mon: mon, minSizeMB: minSize, maxSizeMB: maxSize, workers: workers}
	lg.mode.Store(int32(insertMode))
	return lg
}

func (lg *LoadGen) Run(ctx context.Context) {
	// ensure table exists
	_, _ = lg.pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS test (id bigserial PRIMARY KEY, data text)")

	// запускаем воркеры
	for i := 0; i < lg.workers; i++ {
		go lg.worker(ctx)
	}

	// мониторинг размера и переключение режима
	delta := lg.maxSizeMB - lg.minSizeMB
	margin := delta / 10 // 10% диапазона 

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			size, err := lg.mon.GetDBSize()
			if err != nil {
				fmt.Println("monitor error:", err)
				continue
			}
			fmt.Printf("Current DB size: %d MB\n", size)
			if size <= lg.minSizeMB {
				lg.mode.Store(int32(insertMode))
			} else if size >= lg.maxSizeMB {
				lg.mode.Store(int32(deleteMode))
			} else {
				high := lg.maxSizeMB - margin
				low := lg.minSizeMB + margin
				if size >= high {
					lg.mode.Store(int32(deleteMode))
				} else if size <= low {
					lg.mode.Store(int32(insertMode))
				} else {
					lg.mode.Store(int32(updateMode))
				}
			}
		}
	}
}

func (lg *LoadGen) worker(ctx context.Context) {
	rand.Seed(time.Now().UnixNano())
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		currentMode := modeEnum(lg.mode.Load())
		switch currentMode {
		case insertMode:
			lg.batchInsert(ctx)
		case deleteMode:
			lg.randomDelete(ctx)
		case updateMode:
			lg.randomUpdate(ctx)
		}
	}
}

// batchInsert вставляет до 100 строк, но гарантирует, что после вставки объём не превысит maxSizeMB.
func (lg *LoadGen) batchInsert(ctx context.Context) {
	lg.sizeMu.Lock()
	defer lg.sizeMu.Unlock()

	// узнаём актуальный размер
	sizeMB, err := lg.mon.GetDBSize()
	if err != nil {
		return // при ошибке просто пропустим попытку, попробуем позже
	}

	// если уже упёрлись в максимум – выходим
	if sizeMB >= lg.maxSizeMB {
		time.Sleep(100 * time.Millisecond)
		return
	}

	// сколько байт ещё можем добавить, чтобы не превысить maxSizeMB
	freeBytes := (lg.maxSizeMB - sizeMB) * 1024 * 1024
	allowedRows := int(freeBytes / rowSizeBytes)
	if allowedRows == 0 {
		time.Sleep(100 * time.Millisecond)
		return
	}

	// не более 100 строк за батч, но и не больше, чем позволяет лимит
	if allowedRows > 100 {
		allowedRows = 100
	}

	batch := &pgx.Batch{}
	for i := 0; i < allowedRows; i++ {
		batch.Queue("INSERT INTO test(data) VALUES($1)", randString(int(rowSizeBytes)))
	}
	br := lg.pool.SendBatch(ctx, batch)
	_ = br.Close()
}

// randomDelete удаляет строки так, чтобы не опуститься ниже minSizeMB
func (lg *LoadGen) randomDelete(ctx context.Context) {
	lg.sizeMu.Lock()
	defer lg.sizeMu.Unlock()

	adjust := func() (int64, error) {
		sizeMB, err := lg.mon.GetDBSize()
		if err != nil {
			return 0, err
		}
		if sizeMB <= lg.minSizeMB {
			return sizeMB, nil
		}
		targetMB := (lg.minSizeMB + lg.maxSizeMB) / 2 // пытаемся довести до середины диапазона
		removableBytes := (sizeMB - targetMB) * 1024 * 1024
		removableRows := int(removableBytes / rowSizeBytes)
		if removableRows < 1 {
			removableRows = 1
		}
		if removableRows > 5000 {
			removableRows = 5000
		}
		_, _ = lg.pool.Exec(ctx, fmt.Sprintf("DELETE FROM test WHERE id IN (SELECT id FROM test TABLESAMPLE SYSTEM (10) LIMIT %d)", removableRows))
		_, _ = lg.pool.Exec(ctx, "VACUUM FULL test") // гарантированное сокращение файла
		sizeMB, _ = lg.mon.GetDBSize()
		return sizeMB, nil
	}

	for {
		cur, err := adjust()
		if err != nil {
			return
		}
		if cur <= lg.maxSizeMB && cur >= lg.minSizeMB {
			break
		}
		if cur < lg.minSizeMB {
			// недолет – вернёмся в рабочий режим вставки
			break
		}
	}
}

// randomUpdate выполняет обновления, но контролирует, чтобы не превысить maxSizeMB
func (lg *LoadGen) randomUpdate(ctx context.Context) {
	lg.sizeMu.Lock()
	defer lg.sizeMu.Unlock()

	sizeMB, err := lg.mon.GetDBSize()
	if err != nil {
		return
	}

	if sizeMB >= lg.maxSizeMB {
		// слишком близко к максимуму – пропускаем обновление
		time.Sleep(50 * time.Millisecond)
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

	_, _ = lg.pool.Exec(ctx, fmt.Sprintf("UPDATE test SET data = $1 WHERE id IN (SELECT id FROM test TABLESAMPLE SYSTEM (1) LIMIT %d)", allowedRows), randString(int(rowSizeBytes)))
}

func randString(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
