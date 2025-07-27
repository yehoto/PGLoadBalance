package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	minConnsMultiplier = 4
	maxConnIdleTime    = 5 * time.Minute
)

func ConnectPool(dsn string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	cfg.MaxConns = maxConns
	cfg.MinConns = maxConns / minConnsMultiplier
	cfg.MaxConnIdleTime = maxConnIdleTime

	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, err
	}

	return pool, nil
}
