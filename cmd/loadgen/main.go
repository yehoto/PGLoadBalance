package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"PGLoadBalance/internal/config"
	"PGLoadBalance/internal/loadgen"
	"PGLoadBalance/internal/monitoring"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	minSize := flag.Int64("min-size", 100 , "Minimum database size in MB")
	maxSize := flag.Int64("max-size", 200 , "Maximum database size in MB")
	tablesCount := flag.Int("tables", 1 , "Number of tables to use for load generation")
	flag.Parse()

	if *minSize >= *maxSize {
		fmt.Println("Error: min-size must be less than max-size")
		os.Exit(1)
	}

	// Загрузка конфигурации
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Printf("Error loading configuration: %v\n", err)
		os.Exit(1)
	}

	// Создание пула соединений
	pool, err := pgxpool.New(context.Background(), cfg.Postgres.DSN)
	if err != nil {
		fmt.Printf("Error connecting to database: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Инициализация мониторинга
	mon := monitoring.New(pool)

	// Создание генератора нагрузки
	lg := loadgen.New(pool, mon, *minSize, *maxSize, *tablesCount)

	// Настройка graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Обработка сигналов завершения
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		fmt.Println("\nShutting down gracefully...")
		cancel()
		time.Sleep(1 * time.Second)
	}()

	// Запуск генерации нагрузки
	log.Printf("Starting load generator with %d tables (min=%dMB, max=%dMB)",
		*tablesCount, *minSize, *maxSize)
	lg.Run(ctx)
	log.Println("Load generator stopped")
}
