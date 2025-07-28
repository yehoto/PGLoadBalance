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
	rand             *rand.Rand
}

type LoadGen struct {
	pool      *pgxpool.Pool
	mon       *monitoring.Monitor
	minSizeMB int64
	maxSizeMB int64
	tables    []*TableState
	wg        sync.WaitGroup
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

	 // Запуск горутин для каждой таблицы с интервалом
    for i, table := range lg.tables {
        lg.wg.Add(1)
        go func(t *TableState) {
            defer lg.wg.Done()
            lg.runTable(ctx, t)
        }(table)

        // Добавляем задержку между запуском горутин
        if i < len(lg.tables)-1 {
            select {
            case <-time.After(500 * time.Millisecond):
            case <-ctx.Done():
                return
            }
        }
    }

	lg.wg.Wait()
}

func (lg *LoadGen) runTable(ctx context.Context, table *TableState) {
	ticker := time.NewTicker(333 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
             lg.observer(ctx, table)
			
		}
	}
}

func (lg *LoadGen) observer(ctx context.Context, table *TableState) {
    // Получение размера БД
    dbSizeMB, err := lg.mon.GetDBSize()
			if err != nil {
				log.Printf("Failed to get DB size: %v", err)
			}
	
			// Распределение общего размера БД по таблицам
			n := int64(len(lg.tables))
			dbSizePerTable := (dbSizeMB * 1024 * 1024 )/ n
		
			// Добавление остатка к первой таблице
			if table == lg.tables[0] {
				remainder := (dbSizeMB * 1024 * 1024) % n
				dbSizePerTable += remainder
			}

			lg.checkMaintenance(ctx, table)

			mode := insertMode

			delta := (dbSizePerTable - lg.getTablePhysicalSize(table) )
			minSizeBytes := ((lg.minSizeMB * 1024 * 1024) / n) + delta
			maxSizeBytes := ((lg.maxSizeMB * 1024 * 1024) / n) - delta

			if table == lg.tables[0] {
						remainder1 := (lg.minSizeMB * 1024 * 1024) % n
						remainder2 := (lg.maxSizeMB * 1024 * 1024) % n
						minSizeBytes += remainder1
						maxSizeBytes += remainder2
					}

			  totalTableSize, err := lg.mon.GetTableSize(ctx, table.name)
			if err != nil {
				log.Printf("Failed to get DB size: %v", err)
			}		
             hysteresisBytes := int64(float64(maxSizeBytes - minSizeBytes) * 0.1)//10% от диапазона
			if  totalTableSize -  hysteresisBytes >= maxSizeBytes {
				mode = deleteMode
			} else if  totalTableSize +  hysteresisBytes<= minSizeBytes {
				mode = insertMode
			} else {
				switch table.rand.Intn(3) {
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
				lg.batchInsert(ctx, table, maxSizeBytes)
			case deleteMode:
				lg.randomDelete(ctx, table, minSizeBytes)
			case updateMode:
				lg.batchUpdate(ctx, table, maxSizeBytes,minSizeBytes)
			}
}

func (lg *LoadGen) getTablePhysicalSize(table *TableState) int64 {
    return table.liveTupleBytes.Load() + table.deadTupleBytes.Load() + table.emptyTuplesBytes.Load()
}

func (lg *LoadGen) batchInsert(
	ctx context.Context,
	table *TableState,
	maxSizeBytes int64,
) error {

	if maxSizeBytes <= lg.getTablePhysicalSize(table) {
		log.Printf("NO INSERT maxSizeBytes <= currentSizeBytes")
		return nil
	}

	freeSpaceBytes := maxSizeBytes - lg.getTablePhysicalSize(table) + table.emptyTuplesBytes.Load()
	if freeSpaceBytes <= 0 {
		log.Printf("[%s] No space available for insert", table.name)
		return nil
	}

	maxInsertRows := int(freeSpaceBytes / avgTupleLen)
	if maxInsertRows <= 0 {
		log.Printf("NO INSERT maxInsertRows <= 0 ")
		return nil
	}

	if maxInsertRows > 5000 {
		maxInsertRows = 5000
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
	minSizeBytes int64,
) error {

	toDeleteBytes := lg.getTablePhysicalSize(table) - minSizeBytes - table.deadTupleBytes.Load() - table.emptyTuplesBytes.Load()

	toDeleteTuples := int(toDeleteBytes / avgTupleLen)

	log.Printf("TODELETETUPLRS %d", toDeleteTuples)
	if toDeleteTuples <= 0 {
		toDeleteTuples = 0
	} else if toDeleteTuples > 5000 {
		toDeleteTuples = 5000
	}

	if toDeleteTuples == 0 {
		return nil
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
	freeSpaceBytes := maxSizeBytes - lg.getTablePhysicalSize(table) + table.emptyTuplesBytes.Load()

	maxInsertRows := int(freeSpaceBytes / avgTupleLen)
	toDeleteBytes := lg.getTablePhysicalSize(table) - minSizeBytes - table.deadTupleBytes.Load() - table.emptyTuplesBytes.Load()

	toDeleteTuples := int(toDeleteBytes / avgTupleLen)


	minRows := maxInsertRows
	 if toDeleteTuples < minRows {
		minRows = toDeleteTuples
	 }

	if minRows > 500 {
		minRows = 500
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

	if deadBytes/avgTupleLen >= 1000 {
		log.Printf("[%s] Trigger manual VACUUM", table.name)
        lg.pool.Exec(ctx, queries.Vacuum(table.name))
	}

	n := int64(len(lg.tables))
	rangeSize := (lg.maxSizeMB - lg.minSizeMB) * 1024 * 1024 / n
	if float64(rangeSize)*0.8 <= float64(freeBytes) {
		log.Printf("[%s] Running VACUUM FULL", table.name)
		lg.pool.Exec(ctx, queries.VacuumFull(table.name))
		
			// Обновление статистики после VACUUM FULL
			tupBytes, deadBytes, freeBytes, _ := lg.mon.GetPgstattupleStats(ctx, table.name)
	
			table.liveTupleBytes.Store(tupBytes)
			table.emptyTuplesBytes.Store(freeBytes)
			table.deadTupleBytes.Store(deadBytes)
		
	}
}

