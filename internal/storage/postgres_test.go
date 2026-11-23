package storage

import (
	"context"
	"database/sql"
	"os"
	"reflect"
	"testing"

	"github.com/Arman9128/go-backend-engineering/internal/config"
	"github.com/Arman9128/go-backend-engineering/internal/database"
	"github.com/Arman9128/go-backend-engineering/internal/task"
)

func TestPostgresStorageSaveAndUpdateStatus(t *testing.T) {
	if os.Getenv("POSTGRES_INTEGRATION") != "1" {
		t.Skip("PostgreSQL integration test is disabled")
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	db, err := database.NewPostgresDB(cfg)
	if err != nil {
		t.Fatalf("connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	storage := NewPostgresStorage(db)

	taskData := &task.Task{
		Name:   "integration-test-task",
		Status: task.StatusPending,
		Tags:   []string{"go", "postgres"},
	}

	if err := storage.Save(taskData); err != nil {
		t.Fatalf("save task: %v", err)
	}

	if taskData.ID <= 0 {
		t.Fatalf("expected generated ID, got %d", taskData.ID)
	}

	if taskData.CreatedAt.IsZero() {
		t.Fatal("expected database-generated CreatedAt")
	}

	defer func() {
		_, err := db.Exec("DELETE FROM tasks WHERE id = $1", taskData.ID)
		if err != nil {
			t.Errorf("cleanup task: %v", err)
		}
	}()

	var (
		name      string
		status    string
		tags      []string
		createdAt sql.NullTime
	)

	err = db.QueryRowContext(
		context.Background(),
		`SELECT name, status, tags, created_at FROM tasks WHERE id = $1`,
		taskData.ID,
	).Scan(&name, &status, &tags, &createdAt)

	if err != nil {
		t.Fatalf("query saved task: %v", err)
	}

	if name != taskData.Name {
		t.Errorf("expected name %q, got %q", taskData.Name, name)
	}

	if status != task.StatusPending {
		t.Errorf("expected status %q, got %q", task.StatusPending, status)
	}

	if !reflect.DeepEqual(tags, taskData.Tags) {
		t.Errorf("expected tags %v, got %v", taskData.Tags, tags)
	}

	if !createdAt.Valid {
		t.Fatal("expected valid created_at")
	}

	if err := storage.UpdateStatus(taskData.ID, task.StatusCompleted); err != nil {
		t.Fatalf("update task status: %v", err)
	}

	var updatedStatus string

	err = db.QueryRow(
		"SELECT status FROM tasks WHERE id = $1",
		taskData.ID,
	).Scan(&updatedStatus)

	if err != nil {
		t.Fatalf("query updated status: %v", err)
	}

	if updatedStatus != task.StatusCompleted {
		t.Errorf("expected status %q, got %q", task.StatusCompleted, updatedStatus)
	}
}
