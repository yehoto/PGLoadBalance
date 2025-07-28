package loadgen

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/thanhpk/randstr"

	"PGLoadBalance/internal/monitoring"
	"PGLoadBalance/internal/queries"
)

type modeEnum int

const (
	avgTupleLen          = 1060
	insertMode  modeEnum = iota
	deleteMode
	updateMode
)

type TableState struct {
	name             string
	emptyTuplesBytes atomic.Int64
	liveTupleBytes   atomic.Int64
	deadTupleBytes   atomic.Int64
	delta            atomic.Int64
	rand             *rand.Rand
	minSizeBytes     atomic.Int64
	maxSizeBytes     atomic.Int64
}

type LoadGen struct {
	pool         *pgxpool.Pool
	mon          *monitoring.Monitor
	minSizeMB    int64
	maxSizeMB    int64
	tables       []*TableState
	wg           sync.WaitGroup
	currentDbSize atomic.Int64
	notifyCh     chan struct{}
	mu           sync.Mutex
	// Добавляем барьерный WaitGroup и флаг остановки
	barrierWg    sync.WaitGroup
	shutdown     atomic.Bool
}

func New(pool *pgxpool.Pool, mon *monitoring.Monitor, minSize, maxSize int64, tablesCount int) *LoadGen {
	tables := make([]*TableState, tablesCount)
	for i := 0; i < tablesCount; i++ {
		tables[i] = &TableState{
			name: fmt.Sprintf("test_%d", i),
			rand: rand.New(rand.NewSource(time.Now().UnixNano() + int64(i))),
		}
	}
	return &LoadGen{
		pool:      pool,
		mon:       mon,
		minSizeMB: minSize,
		maxSizeMB: maxSize,
		tables:    tables,
		notifyCh:  make(chan struct{}),
		shutdown:  atomic.Bool{},
	}
}

func (lg *LoadGen) Run(ctx context.Context) {
	_, err := lg.pool.Exec(ctx, queries.CreateExtensionPgstattuple)
	if err != nil {
		log.Printf("Failed to create extension: %v", err)
	}

	for _, table := range lg.tables {
		_, err := lg.pool.Exec(ctx, queries.CreateTable(table.name))
		if err != nil {
			log.Printf("Failed to create table %s: %v", table.name, err)
			return
		}
		_, err = lg.pool.Exec(ctx, queries.DisableAutovacuum(table.name))
		if err != nil {
			log.Printf("Failed to disable autovacuum: %v", err)
		}
	}

	 // Первоначальный запрос размера БД
	dbSizeMB, err := lg.mon.GetDBSize()
	if err == nil {
		lg.currentDbSize.Store(dbSizeMB)
	}

	// Запуск sizePoller
	lg.wg.Add(1)
	go func() {
		defer lg.wg.Done()
		lg.sizePoller(ctx)
	}()

	// Запуск горутин для таблиц
	for _, table := range lg.tables {
		lg.wg.Add(1)
		go func(t *TableState) {
			defer lg.wg.Done()
			lg.runTable(ctx, t)
		}(table)
	}
	
	lg.wg.Wait()
}

func (lg *LoadGen) sizePoller(ctx context.Context) {
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			lg.shutdown.Store(true)
			lg.mu.Lock()
			if lg.notifyCh != nil {
				close(lg.notifyCh)
				lg.notifyCh = nil
			}
			lg.mu.Unlock()
			return
		case <-ticker.C:
			dbSizeMB, err := lg.mon.GetDBSize()
			if err != nil {
				log.Printf("Failed to get DB size: %v", err)
				continue
			}
			lg.currentDbSize.Store(dbSizeMB)

			// Инициализируем барьер
			lg.barrierWg.Add(len(lg.tables))
			
			// Отправляем уведомление
			lg.mu.Lock()
			oldCh := lg.notifyCh
			lg.notifyCh = make(chan struct{})
			lg.mu.Unlock()
			
			if oldCh != nil {
				close(oldCh)
			}
			
			// Ждем завершения обработки всеми горутинами
			done := make(chan struct{})
			go func() {
				lg.barrierWg.Wait()
				close(done)
			}()
			
			// С таймаутом на случай зависаний
			select {
			case <-done:
				// Все горутины завершили обработку
			case <-time.After(2 * time.Second):
				log.Println("Timeout waiting for table workers")
			case <-ctx.Done():
				lg.shutdown.Store(true)
				return
			}
		}
	}
}

func (lg *LoadGen) runTable(ctx context.Context, table *TableState) {
	for {
		if lg.shutdown.Load() {
			return
		}

		// Получаем текущий канал уведомлений
		lg.mu.Lock()
		ch := lg.notifyCh
		lg.mu.Unlock()
		
		// Ожидаем уведомления или завершения
		select {
		case <-ctx.Done():
			return
		case <-ch:
		}

		// Выполняем обработку
		dbSizeMB := lg.currentDbSize.Load()
		lg.observer(ctx, table, dbSizeMB)
		
		// Уведомляем о завершении обработки
		lg.barrierWg.Done()
	}
}

func (lg *LoadGen) observer(ctx context.Context, table *TableState, dbSizeMB int64) {
		lg.checkMaintenance(ctx, table)
	// Распределение общего размера БД по таблицам
	n := int64(len(lg.tables))
	dbSizePerTable := (dbSizeMB * 1024 * 1024) / n

	// Добавление остатка к первой таблице
	if table == lg.tables[0] {
		remainder := (dbSizeMB * 1024 * 1024) % n
		dbSizePerTable += remainder
	}
   	log.Printf("DNSIZEMB %d", dbSizeMB)
		//log.Printf("perrrrr %d", dbSizePerTable)
	//

	mode := insertMode
	delta := (dbSizePerTable - lg.getTablePhysicalSize(table))
	table.delta.Store(delta)
	minSizeBytes := (lg.minSizeMB * 1024 * 1024) / n
	maxSizeBytes := (lg.maxSizeMB * 1024 * 1024) / n

	if table == lg.tables[0] {
		remainder1 := (lg.minSizeMB * 1024 * 1024) % n
		remainder2 := (lg.maxSizeMB * 1024 * 1024) % n
		minSizeBytes += remainder1
		maxSizeBytes += remainder2
	}

	table.minSizeBytes.Store(minSizeBytes)
	table.maxSizeBytes.Store(maxSizeBytes)

	
	hys := float64(maxSizeBytes - minSizeBytes) * 0.1  // 10% гистерезис
aggressive := table.rand.Float64() < 0.3  // 30% шанс агрессивного действия

switch {
case dbSizePerTable < minSizeBytes + int64(hys):
    mode = insertMode
    if aggressive {
        mode = insertMode // Агрессивная вставка
    }
case dbSizePerTable > maxSizeBytes - int64(hys):
    mode = deleteMode
    if aggressive {
        mode = deleteMode // Агрессивное удаление
    }
default:
    // Случайный выбор с приоритетом на update
    rnd := table.rand.Intn(10)
    switch {
    case rnd < 4: // 40% вероятность
        mode = updateMode
    case rnd < 7: // 30% вероятность
        mode = insertMode
    default: // 30% вероятность
        mode = deleteMode
    }
}

	switch mode {
	case insertMode:
		lg.batchInsert(ctx, table)
	case deleteMode:
		lg.randomDelete(ctx, table)
	case updateMode:
		lg.batchUpdate(ctx, table, maxSizeBytes, minSizeBytes)
	}
}


func (lg *LoadGen) getTablePhysicalSize(table *TableState) int64 {
    return table.liveTupleBytes.Load() + table.deadTupleBytes.Load() + table.emptyTuplesBytes.Load()
}

func (lg *LoadGen) batchInsert(
	ctx context.Context,
	table *TableState,
) error {

	if table.maxSizeBytes.Load() <= lg.getTablePhysicalSize(table){
		log.Printf("NO INSERT maxSizeBytes <= currentSizeBytes")
		return nil
	}
delta := table.delta.Load()
if (delta < 0 ){
	delta = 0
}
	

	freeSpaceBytes := table.maxSizeBytes.Load() - table.deadTupleBytes.Load() - table.emptyTuplesBytes.Load() - delta

	if freeSpaceBytes <= 0 {
		log.Printf("[%s] No space available for insert", table.name)
		return nil
	}

	maxInsertRows := int(freeSpaceBytes / avgTupleLen)
	if maxInsertRows <= 0 {
		log.Printf("NO INSERT maxInsertRows <= 0 ")
		return nil
	}

	maxAllowedToInsert := int(float64(table.maxSizeBytes.Load() - table.minSizeBytes.Load()) * 0.0253 / avgTupleLen)

	if maxInsertRows > maxAllowedToInsert {
		maxInsertRows =maxAllowedToInsert
	}
	log.Printf("MAXINSERTROWS %d ", maxInsertRows)

	rowsToInsert := 1
	if maxInsertRows > 1 {
		rowsToInsert = table.rand.Intn(maxInsertRows) + 1
	}

	batch := &pgx.Batch{}
	for i := 0; i < rowsToInsert; i++ {
		batch.Queue(queries.Insert(table.name), randstr.String(1024))
	}

	if err := lg.pool.SendBatch(ctx, batch).Close(); err != nil {
		return fmt.Errorf("batch insert failed for %s: %w", table.name, err)
	}

	log.Printf("[%s] Inserted rows: %d", table.name, rowsToInsert)
	return nil
}

func (lg *LoadGen) randomDelete(
	ctx context.Context,
	table *TableState,
) error {

	if (table.liveTupleBytes.Load()<=table.minSizeBytes.Load()){

       lg.pool.Exec(ctx, queries.Vacuum(table.name))
		return nil
	}
	toDeleteBytes := table.liveTupleBytes.Load() - table.minSizeBytes.Load()
	toDeleteTuples := int(toDeleteBytes / avgTupleLen)

	log.Printf("TODELETETUPLRS %d liveTupleByte %d minSizeBytes %d delta %d", toDeleteTuples, table.liveTupleBytes.Load(),table.minSizeBytes.Load(), table.delta.Load() )
	 maxAllowedDelete := int(float64(table.maxSizeBytes.Load() - table.minSizeBytes.Load()) * 0.0253 / avgTupleLen)

	if toDeleteTuples <= 0 {
		return nil
	} else if toDeleteTuples > maxAllowedDelete {
		toDeleteTuples = maxAllowedDelete
	}
	rowsToDelete := table.rand.Intn(toDeleteTuples) + 1
	
	query := queries.Delete(table.name, rowsToDelete)
	res, err := lg.pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("delete failed for %s: %w", table.name, err)
	}

	log.Printf("[%s] Deleted rows: %d", table.name, res.RowsAffected())
	return nil
}

func (lg *LoadGen) batchUpdate(ctx context.Context, table *TableState, maxSizeBytes int64, minSizeBytes int64) error {
	// меньшее между строк что можно удалить и добавить
	// freeSpaceBytes := maxSizeBytes - lg.getTablePhysicalSize(table) + table.emptyTuplesBytes.Load()/2 - table.delta.Load()
	// freeSpaceBytes = int64(float64(freeSpaceBytes)*0.7)

	// maxInsertRows := int(freeSpaceBytes / avgTupleLen)
	// toDeleteBytes := lg.getTablePhysicalSize(table) - minSizeBytes - table.deadTupleBytes.Load() - table.emptyTuplesBytes.Load() + table.delta.Load()/2
    // toDeleteBytes = int64(float64(toDeleteBytes)*0.7)
	// toDeleteTuples := int(toDeleteBytes / avgTupleLen)

    
	// minRows := maxInsertRows
	//  if toDeleteTuples < minRows {
	// 	minRows = toDeleteTuples
	//  }
	minRows:=1
	 maxAllowedToUpdate := int(float64(table.maxSizeBytes.Load() - table.minSizeBytes.Load()) * 0.0253 / avgTupleLen)

	if minRows > maxAllowedToUpdate {
		minRows = maxAllowedToUpdate
	}

	if minRows <= 0 {
		return nil
	}


	rowsToUpdate := int(minRows)
	if rowsToUpdate > 1 {
		rowsToUpdate = table.rand.Intn(rowsToUpdate) + 1
	}

	query := queries.Update(table.name, rowsToUpdate)
	res, err := lg.pool.Exec(ctx, query, randstr.String(1024))
	if err != nil {
		return fmt.Errorf("update failed for %s: %w", table.name, err)
	}

	log.Printf("[%s] Updated rows: %d", table.name, res.RowsAffected())
	return nil
}

func (lg *LoadGen) checkMaintenance(ctx context.Context, table *TableState) {
	tupBytes, deadBytes, freeBytes, err := lg.mon.GetPgstattupleStats(ctx, table.name)
	if err != nil {
		log.Printf("[%s] pgstattuple failed: %v", table.name, err)
		return
	}
	table.liveTupleBytes.Store(tupBytes)
	table.emptyTuplesBytes.Store(freeBytes)
	table.deadTupleBytes.Store(deadBytes)
   vacuumStart := int(float64(table.maxSizeBytes.Load() - table.minSizeBytes.Load()) * 0.005 / avgTupleLen)
	if float64(deadBytes/avgTupleLen) >= float64(vacuumStart) {
		log.Printf("[%s] Trigger manual VACUUM", table.name)
        lg.pool.Exec(ctx, queries.Vacuum(table.name))
		 // Обновите данные сразу после VACUUM!
        tupBytes, deadBytes, freeBytes, _ := lg.mon.GetPgstattupleStats(ctx, table.name)
        table.liveTupleBytes.Store(tupBytes)
        table.deadTupleBytes.Store(deadBytes)
        table.emptyTuplesBytes.Store(freeBytes)
	}

	n := int64(len(lg.tables))
	rangeSize := (lg.maxSizeMB - lg.minSizeMB) * 1024 * 1024 / n
	if float64(rangeSize)*0.8 <= float64(freeBytes) {
		log.Printf("[%s] Running VACUUM FULL", table.name)
		lg.pool.Exec(ctx, queries.VacuumFull(table.name))
		
			tupBytes, deadBytes, freeBytes, _ := lg.mon.GetPgstattupleStats(ctx, table.name)
	
			table.liveTupleBytes.Store(tupBytes)
			table.emptyTuplesBytes.Store(freeBytes)
			table.deadTupleBytes.Store(deadBytes)
		
	}
}

