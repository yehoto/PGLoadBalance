package monitoring

import (
	"context"

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
	return size / (1024 * 1024), err // в MB
}
