package postgres

import (
	"context"
	"time"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ConnectPool создаёт пул соединений pgxpool с указанным максимальным количеством коннектов.
func ConnectPool(dsn string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns
	cfg.MinConns = maxConns / 4// хотя бы столько соединений
	cfg.MaxConnIdleTime = 5 * time.Minute//если соединение не используется более 5 минут - оно будет закрыто
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, err
	}
	// проверка подключения
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}
