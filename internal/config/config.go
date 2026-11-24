package config

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	HTTPAddr    string
	RedisAddr   string
	WorkerCount int
	DBHost      string
	DBPort      int
	DBUser      string
	DBPassword  string
	DBName      string
	DBSSLMode   string
}

func Load() (*Config, error) {
	if err := godotenv.Load(); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("load environment file: %w", err)
		}
	}

	port, err := strconv.Atoi(os.Getenv("DB_PORT"))
	if err != nil || port < 1 || port > 65535 {
		return nil, fmt.Errorf("invalid DB_PORT")
	}

	httpAddr := os.Getenv("HTTP_ADDR")
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	if httpAddr == "" {
		httpAddr = ":8080"
	}

	workerCount := 3
	if value := os.Getenv("WORKER_COUNT"); value != "" {
		workerCount, err = strconv.Atoi(value)
		if err != nil || workerCount <= 0 {
			return nil, fmt.Errorf("invalid WORKER_COUNT: must be a positive integer")
		}
	}

	cfg := &Config{
		HTTPAddr:    httpAddr,
		RedisAddr:   redisAddr,
		WorkerCount: workerCount,
		DBHost:      os.Getenv("DB_HOST"),
		DBPort:      port,
		DBUser:      os.Getenv("DB_USER"),
		DBPassword:  os.Getenv("DB_PASSWORD"),
		DBName:      os.Getenv("DB_NAME"),
		DBSSLMode:   os.Getenv("DB_SSLMODE"),
	}

	if cfg.DBHost == "" {
		return nil, fmt.Errorf("DB_HOST is required")
	}

	if cfg.DBUser == "" {
		return nil, fmt.Errorf("DB_USER is required")
	}

	if cfg.DBPassword == "" {
		return nil, fmt.Errorf("DB_PASSWORD is required")
	}

	if cfg.DBName == "" {
		return nil, fmt.Errorf("DB_NAME is required")
	}

	if cfg.DBSSLMode == "" {
		return nil, fmt.Errorf("DB_SSLMODE is required")
	}

	return cfg, nil
}
