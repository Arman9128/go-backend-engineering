package database

import (
	"context"
	"os"
	"testing"

	"github.com/Arman9128/go-backend-engineering/internal/config"
)

func TestPostgresConnection(t *testing.T) {
	if os.Getenv("POSTGRES_INTEGRATION") != "1" {
		t.Skip("PostgreSQL integration test is disabled")
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	db, err := NewPostgresDB(cfg)
	if err != nil {
		t.Fatalf("connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	var result int

	err = db.QueryRowContext(context.Background(), "SELECT 1").Scan(&result)
	if err != nil {
		t.Fatalf("execute query: %v", err)
	}

	if result != 1 {
		t.Fatalf("expected 1, got %d", result)
	}
}
