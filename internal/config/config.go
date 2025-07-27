package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Postgres PostgresConfig `yaml:"postgres"`
}

type PostgresConfig struct {
	DSN string `yaml:"dsn"`
}

func Load(path string) (*Config, error) {
	cfg := &Config{}

	data, err := os.ReadFile(path)
	if err != nil {

		if os.IsNotExist(err) {

		} else {

			return nil, fmt.Errorf("error reading config file: %w", err)
		}
	} else {

		err = yaml.Unmarshal(data, cfg)
		if err != nil {
			return nil, fmt.Errorf("error parsing YAML config: %w", err)
		}
	}

	if envDSN := os.Getenv("DSN"); envDSN != "" {
		cfg.Postgres.DSN = envDSN
	}

	if cfg.Postgres.DSN == "" {
		return nil, fmt.Errorf("DSN is not set: config file %s missing or empty and environment variable DSN is not provided", path)
	}

	return cfg, nil
}
