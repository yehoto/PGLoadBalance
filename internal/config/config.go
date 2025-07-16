package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Postgres struct {
		DSN string `yaml:"dsn"`
	} `yaml:"postgres"`
}

func Load(path string) (*Config, error) {
	cfg := &Config{}

	// Попытка прочитать YAML-конфиг
	if data, err := os.ReadFile(path); err == nil {
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) { // любая ошибка, кроме «файл не найден»
		return nil, err
	}

	// Приоритет у переменной окружения DSN
	if envDSN := os.Getenv("DSN"); envDSN != "" {
		cfg.Postgres.DSN = envDSN
	}

	// Проверяем, что DSN задан
	if cfg.Postgres.DSN == "" {
		return nil, fmt.Errorf("DSN is not set: config file %s missing or empty and environment variable DSN is not provided", path)
	}

	return cfg, nil
}
