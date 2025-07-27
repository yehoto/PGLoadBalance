package monitoring

import (
	"context"
	"time"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Monitoring struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Monitoring {
	return &Monitoring{pool: pool}
}

func (m *Monitoring) GetDBSize() (int64, error) {
	var size int64
	err := m.pool.QueryRow(context.Background(), "SELECT pg_database_size(current_database())").Scan(&size)
	return size / (1024 * 1024), err // в MB
}

var lastTxCount int64
var lastTime time.Time

func (m *Monitoring) GetTPS() (float64, error) {
	var txCount int64
	err := m.pool.QueryRow(context.Background(), "SELECT sum(xact_commit + xact_rollback) FROM pg_stat_database").Scan(&txCount)
	if err != nil {
		return 0, err
	}
	if lastTime.IsZero() {
		lastTxCount = txCount
		lastTime = time.Now()
		return 0, nil
	}
	deltaTx := txCount - lastTxCount
	deltaTime := time.Since(lastTime).Seconds()
	lastTxCount = txCount
	lastTime = time.Now()
	return float64(deltaTx) / deltaTime, nil
}