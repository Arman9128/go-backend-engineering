package task

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type Storage interface {
	Save(task *Task) error
	GetByID(id int64) (Task, error)
	UpdateStatus(id int64, status string) error
}

type ContextStorage interface {
	GetByIDContext(ctx context.Context, id int64) (Task, error)
}

type TaskCompletionStorage interface {
	CompleteTaskContext(ctx context.Context, id int64) error
}

type Service struct {
	Name    string
	storage Storage
}

func NewService(name string, storage Storage) *Service {
	return &Service{
		Name:    name,
		storage: storage,
	}
}

func (s *Service) SubmitTask(taskName string) (*Task, error) {
	return s.SubmitTaskWithTags(taskName, nil)
}

func (s *Service) SubmitTaskWithTags(taskName string, tags []string) (*Task, error) {
	taskName = strings.TrimSpace(taskName)
	if taskName == "" {
		return nil, fmt.Errorf("task name cannot be empty")
	}

	newTask := &Task{
		Name:      taskName,
		Status:    StatusPending,
		CreatedAt: time.Now().UTC(),
		Tags:      append([]string(nil), tags...),
	}

	if err := s.storage.Save(newTask); err != nil {
		return nil, fmt.Errorf("submit task %q: %w", taskName, err)
	}

	return newTask, nil
}

func (s *Service) GetTaskByID(id int64) (Task, error) {
	return s.GetTaskByIDContext(context.Background(), id)
}

func (s *Service) GetTaskByIDContext(ctx context.Context, id int64) (Task, error) {
	if err := ctx.Err(); err != nil {
		return Task{}, err
	}

	if id <= 0 {
		return Task{}, fmt.Errorf("get task: invalid ID %d", id)
	}

	var foundTask Task
	var err error

	if storage, ok := s.storage.(ContextStorage); ok {
		foundTask, err = storage.GetByIDContext(ctx, id)
	} else {
		foundTask, err = s.storage.GetByID(id)
	}

	if err != nil {
		return Task{}, fmt.Errorf("get task %d: %w", id, err)
	}

	return foundTask, nil
}

func (s *Service) UpdateTaskStatus(id int64, status string) error {
	if id <= 0 {
		return fmt.Errorf("update task status: invalid ID %d", id)
	}

	if !IsValidStatus(status) {
		return fmt.Errorf("update task status %q: %w", status, ErrInvalidStatus)
	}

	if err := s.storage.UpdateStatus(id, status); err != nil {
		return fmt.Errorf("update task %d status: %w", id, err)
	}

	return nil
}

func (s *Service) CompleteTaskContext(ctx context.Context, id int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if id <= 0 {
		return fmt.Errorf("complete task: invalid ID %d", id)
	}

	storage, ok := s.storage.(TaskCompletionStorage)
	if !ok {
		return fmt.Errorf("complete task %d: storage does not support atomic completion", id)
	}

	if err := storage.CompleteTaskContext(ctx, id); err != nil {
		return fmt.Errorf("complete task %d: %w", id, err)
	}

	return nil
}
