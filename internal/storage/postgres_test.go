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

func TestPostgresStorageSaveRollsBackWhenOutboxInsertFails(t *testing.T) {
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

	ctx := context.Background()

	_, err = db.ExecContext(ctx, `
                CREATE OR REPLACE FUNCTION test_fail_outbox_insert()
                RETURNS trigger AS $$
                BEGIN
                        RAISE EXCEPTION 'forced outbox insert failure';
                END;
                $$ LANGUAGE plpgsql
        `)
	if err != nil {
		t.Fatalf("create test trigger function: %v", err)
	}

	defer func() {
		_, err := db.ExecContext(ctx,
			"DROP FUNCTION IF EXISTS test_fail_outbox_insert()")
		if err != nil {
			t.Errorf("drop test trigger function: %v", err)
		}
	}()

	_, err = db.ExecContext(ctx, `
                CREATE TRIGGER test_fail_outbox_insert_trigger
                BEFORE INSERT ON outbox_events
                FOR EACH ROW
                EXECUTE FUNCTION test_fail_outbox_insert()
        `)
	if err != nil {
		t.Fatalf("create test trigger: %v", err)
	}

	defer func() {
		_, err := db.ExecContext(ctx, `
                        DROP TRIGGER IF EXISTS test_fail_outbox_insert_trigger
                        ON outbox_events
                `)
		if err != nil {
			t.Errorf("drop test trigger: %v", err)
		}
	}()

	taskData := &task.Task{
		Name:   "rollback-integration-test",
		Status: task.StatusPending,
		Tags:   []string{"rollback"},
	}

	storage := NewPostgresStorage(db)

	err = storage.Save(taskData)
	if err == nil {
		t.Fatal("expected Save to fail when outbox insert fails")
	}

	var count int

	err = db.QueryRowContext(ctx, `
                SELECT COUNT(*)
                FROM tasks
                WHERE name = $1
        `, taskData.Name).Scan(&count)

	if err != nil {
		t.Fatalf("query task after rollback: %v", err)
	}

	if count != 0 {
		t.Errorf("expected task to be rolled back, found %d records", count)
	}
}
