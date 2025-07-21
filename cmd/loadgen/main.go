package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"PGLoadBalance/internal/config"
	"PGLoadBalance/internal/loadgen"
	"PGLoadBalance/internal/monitoring"
	"PGLoadBalance/internal/postgres"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Путь к файлу конфигурации")
	minSize := flag.Int64("min-size", 100, "Минимальный размер базы данных в МБ")
	maxSize := flag.Int64("max-size", 300, "Максимальный размер базы данных в МБ")
	workers := flag.Int("workers", 16, "Количество воркеров")
	numTables := flag.Int("tables", 4, "Количество тестовых таблиц")
	flag.Parse()

	// Проверка параметров
	if *minSize >= *maxSize {
		fmt.Println("Ошибка: min-size должен быть меньше max-size")
		os.Exit(1)
	}
	if *numTables < 1 {
		fmt.Println("Ошибка: tables должно быть >= 1")
		os.Exit(1)
	}

	// Загрузка конфигурации
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Printf("Ошибка загрузки конфигурации: %v\n", err)
		os.Exit(1)
	}

	// Создание пула соединений
	pool, err := postgres.ConnectPool(cfg.Postgres.DSN, 50)
	if err != nil {
		fmt.Printf("Ошибка подключения к БД: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Инициализация мониторинга
	mon := monitoring.New(pool)

	// Создание и запуск генератора нагрузки
	lg := loadgen.New(pool, mon, *minSize, *maxSize, *workers, *numTables)
	ctx := context.Background()
	lg.Run(ctx)
}