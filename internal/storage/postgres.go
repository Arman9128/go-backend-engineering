package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Arman9128/go-backend-engineering/internal/task"
)

type PostgresStorage struct {
	db *sql.DB
}

func NewPostgresStorage(db *sql.DB) *PostgresStorage {
	return &PostgresStorage{
		db: db,
	}
}

func (s *PostgresStorage) Save(taskData *task.Task) error {
	if taskData == nil {
		return fmt.Errorf("save task: task is nil")
	}

	tags := taskData.Tags
	if tags == nil {
		tags = []string{}
	}

	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return fmt.Errorf("begin task transaction: %w", err)
	}
	defer tx.Rollback()

	var taskID int64
	var createdAt time.Time

	err = tx.QueryRow(`
		INSERT INTO tasks (name, status, tags)
		VALUES ($1, $2, $3)
		RETURNING id, created_at
	`, taskData.Name, taskData.Status, tags).Scan(
		&taskID,
		&createdAt,
	)
	if err != nil {
		return fmt.Errorf("save task in PostgreSQL: %w", err)
	}

	payload, err := json.Marshal(map[string]any{
		"id":         taskID,
		"name":       taskData.Name,
		"status":     taskData.Status,
		"created_at": createdAt,
		"tags":       tags,
	})
	if err != nil {
		return fmt.Errorf("marshal task event: %w", err)
	}

	_, err = tx.Exec(`
		INSERT INTO outbox_events (
			aggregate_type, aggregate_id, event_type, payload
		)
		VALUES ($1, $2, $3, $4)
	`, "task", taskID, "task.created", string(payload))
	if err != nil {
		return fmt.Errorf("save task event in outbox: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit task transaction: %w", err)
	}

	taskData.ID = taskID
	taskData.CreatedAt = createdAt

	return nil
}

func (s *PostgresStorage) GetByID(id int64) (task.Task, error) {
	return s.GetByIDContext(context.Background(), id)
}

func (s *PostgresStorage) GetByIDContext(ctx context.Context, id int64) (task.Task, error) {
	var foundTask task.Task

	query := `
		SELECT id, name, status, created_at, tags
		FROM tasks
		WHERE id = $1
	`

	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&foundTask.ID,
		&foundTask.Name,
		&foundTask.Status,
		&foundTask.CreatedAt,
		&foundTask.Tags,
	)

	if errors.Is(err, sql.ErrNoRows) {
		return task.Task{}, fmt.Errorf("get task %d: %w", id, task.ErrNotFound)
	}
	if err != nil {
		return task.Task{}, fmt.Errorf("get task %d from PostgreSQL: %w", id, err)
	}

	return foundTask, nil
}

func (s *PostgresStorage) CompleteTaskContext(ctx context.Context, id int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin task completion transaction: %w", err)
	}
	defer tx.Rollback()

	var currentStatus string
	err = tx.QueryRowContext(ctx, `
		SELECT status
		FROM tasks
		WHERE id = $1
		FOR UPDATE
	`, id).Scan(&currentStatus)

	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("complete task %d: %w", id, task.ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("get task %d status: %w", id, err)
	}

	if currentStatus == task.StatusCompleted {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit task completion transaction: %w", err)
		}
		return nil
	}

	if currentStatus != task.StatusPending {
		return fmt.Errorf(
			"complete task %d from status %q: %w",
			id,
			currentStatus,
			task.ErrInvalidTransition,
		)
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE tasks
		SET status = $1
		WHERE id = $2
	`, task.StatusCompleted, id)
	if err != nil {
		return fmt.Errorf("complete task %d in PostgreSQL: %w", id, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit task completion transaction: %w", err)
	}

	return nil
}

func (s *PostgresStorage) UpdateStatus(id int64, status string) error {
	if !task.IsValidStatus(status) {
		return fmt.Errorf("update task status: invalid status %q", status)
	}

	query := `
		UPDATE tasks
		SET status = $1
		WHERE id = $2
	`

	result, err := s.db.Exec(query, status, id)
	if err != nil {
		return fmt.Errorf("update task %d status in PostgreSQL: %w", id, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get affected rows: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("update task %d: %w", id, task.ErrNotFound)
	}

	return nil
}
