package storage

import (
	"database/sql"
	"errors"
	"fmt"

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

	query := `
		INSERT INTO tasks (name, status, tags)
		VALUES ($1, $2, $3)
		RETURNING id, created_at
	`

	err := s.db.QueryRow(
		query,
		taskData.Name,
		taskData.Status,
		tags,
	).Scan(&taskData.ID, &taskData.CreatedAt)

	if err != nil {
		return fmt.Errorf("save task in PostgreSQL: %w", err)
	}

	return nil
}

func (s *PostgresStorage) GetByID(id int64) (task.Task, error) {
	var foundTask task.Task

	query := `
		SELECT id, name, status, created_at, tags
		FROM tasks
		WHERE id = $1
	`

	err := s.db.QueryRow(query, id).Scan(
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
