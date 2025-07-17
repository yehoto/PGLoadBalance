package postgres

import (
	"context"
	"github.com/jackc/pgx/v5"
)

// Connect принимает DSN как строку
func Connect(dsn string) (*pgx.Conn, error) {
	return pgx.Connect(context.Background(), dsn)
}
