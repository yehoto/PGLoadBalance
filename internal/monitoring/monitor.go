package monitoring

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Monitor struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Monitor {
	return &Monitor{pool: pool}
}

func (m *Monitor) GetDBSize() (int64, error) {
	var size int64
	err := m.pool.QueryRow(context.Background(), "SELECT pg_database_size(current_database())").Scan(&size)
	return size / (1024 * 1024), err
}

func (m *Monitor) GetTableSize(ctx context.Context, tableName string) (int64, error) {
	var totalSize int64
	err := m.pool.QueryRow(ctx, "SELECT pg_total_relation_size($1)", tableName).Scan(&totalSize)
	return totalSize, err
}

func (m *Monitor) GetPgstattupleStats(ctx context.Context, tableName string) (tupleLenBytes, deadLenBytes, freeSpaceBytes int64, err error) {
	query := fmt.Sprintf(`
        SELECT
            tuple_len,
            dead_tuple_len,
            free_space
        FROM pgstattuple('%s')`, tableName)

	err = m.pool.QueryRow(ctx, query).Scan(
		&tupleLenBytes,
		&deadLenBytes,
		&freeSpaceBytes,
	)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("pgstattuple scan failed: %w", err)
	}
	return
}
