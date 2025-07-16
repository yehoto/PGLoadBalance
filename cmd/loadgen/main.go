package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"PGLoadBalance/internal/config"
	"PGLoadBalance/internal/loadgen"
	"PGLoadBalance/internal/monitoring"
	"PGLoadBalance/internal/postgres"
)

//метрики для Prometheus
var (
	dbSizeGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pgloadbalance_db_size_mb",
		Help: "Current database size in MB.",
	})
	tpsGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pgloadbalance_db_tps",
		Help: "Transactions per second (commit + rollback).",
	})
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

	// регистрируем метрики
	prometheus.MustRegister(dbSizeGauge, tpsGauge)

	// запускаем HTTP-сервер для /metrics
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Fatalf("metrics server error: %v", err)
		}
	}()

	// Фоновое обновление метрик каждую секунду
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if size, err := mon.GetDBSize(); err == nil {
				dbSizeGauge.Set(float64(size))
			}
			if tps, err := mon.GetTPS(); err == nil {
				tpsGauge.Set(tps)
			}
		}
	}()
//Создание и запуск генератора нагрузки
	lg := loadgen.New(pool, mon, *minSize, *maxSize, *workers)
	ctx := context.Background()
	lg.Run(ctx)
}
