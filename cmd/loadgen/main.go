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
	//Парсинг флагов коммандной строки + значения по умолчанию
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	minSize := flag.Int64("min-size", 100, "Minimum database size in MB")
	maxSize := flag.Int64("max-size", 300, "Maximum database size in MB")
	workers := flag.Int("workers", 16, "Number of concurrent workers")
	flag.Parse()

	//Загрузка конфигурации
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Printf("Error loading config: %v\n", err)
		os.Exit(1)
	}
	
    //Создание пула соединений с PostgreSQL
	pool, err := postgres.ConnectPool(cfg.Postgres.DSN, 50)
	if err != nil {
		fmt.Printf("Error connecting to DB: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()//Автоматические закрытие при выходе
    //Инициализация системы мониторинга
	mon := monitoring.New(pool)
	
//Создание и запуск генератора нагрузки
	lg := loadgen.New(pool, mon, *minSize, *maxSize, *workers)
	ctx := context.Background()
	lg.Run(ctx)
}
