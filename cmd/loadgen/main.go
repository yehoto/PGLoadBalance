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
    // Определение флагов командной строки
    configPath := flag.String("config", "config.yaml", "Path to configuration file")
    minSize := flag.Int64("min-size", 100, "Minimum database size in MB")
    maxSize := flag.Int64("max-size", 500, "Maximum database size in MB")
    tablesCount := flag.Int("tables", 2, "Number of tables to use for load generation")
    flag.Parse()

    // Проверка логики min/max size
    if *minSize >= *maxSize {
        fmt.Println("Error: min-size must be less than max-size")
        os.Exit(1)
    }

    // --- Загрузка конфигурации ---
    cfg, err := config.Load(*configPath)
    if err != nil {
        fmt.Printf("Error loading configuration file '%s': %v\n", *configPath, err)
        os.Exit(1)
    }
    // --- Конец загрузки конфигурации ---

    // --- Определение DSN ---
    // Приоритет: 1. Переменная окружения DSN, 2. DSN из config.yaml
    dsn := cfg.Postgres.DSN // Начинаем с DSN из файла конфигурации
    if envDSN := os.Getenv("DSN"); envDSN != "" {
        log.Println("DSN overridden by environment variable 'DSN'")
        dsn = envDSN
    }

    // Проверка, что DSN не пустой
    if dsn == "" {
        fmt.Println("Error: Database DSN is not set. Please provide it via the 'DSN' environment variable or in the config file.")
        os.Exit(1)
    }
    log.Printf("Using database DSN: %s", dsn) // ВАЖНО: Не выводите логины/пароли в production!
    // --- Конец определения DSN ---

    // --- Создание пула соединений ---
    pool, err := pgxpool.New(context.Background(), dsn)
    if err != nil {
        fmt.Printf("Error creating database connection pool: %v\n", err)
        os.Exit(1)
    }
    defer pool.Close()
    log.Println("Successfully connected to the database.")
    // --- Конец создания пула соединений ---

    // --- Инициализация мониторинга ---
    mon := monitoring.New(pool)
    // --- Конец инициализации мониторинга ---

    // --- Создание генератора нагрузки ---
    lg := loadgen.New(pool, mon, *minSize, *maxSize, *tablesCount)
    // --- Конец создания генератора нагрузки ---

    // --- Настройка graceful shutdown ---
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Горутина для обработки сигналов завершения
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    go func() {
        sig := <-sigChan
        log.Printf("\nReceived signal '%s'. Shutting down gracefully...", sig)
        cancel()
        // Даем немного времени на завершение текущих операций
        time.Sleep(1 * time.Second)
    }()
    // --- Конец настройки graceful shutdown ---

    // --- Запуск генерации нагрузки ---
    log.Printf("Starting load generator with %d tables (min=%dMB, max=%dMB)", *tablesCount, *minSize, *maxSize)
    lg.Run(ctx)
    log.Println("Load generator stopped.")
    // --- Конец запуска генерации нагрузки ---
}