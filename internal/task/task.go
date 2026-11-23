package task

import (
	"fmt"
	"time"
)

type Storage interface {
	Save(task *Task) error
	UpdateStatus(id int64, status string) error
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
	if taskName == "" {
		return nil, fmt.Errorf("task name cannot be empty")
	}

	newTask := &Task{
		Name:      taskName,
		Status:    StatusPending,
		CreatedAt: time.Now().UTC(),
	}

	if err := s.storage.Save(newTask); err != nil {
		return nil, fmt.Errorf("submit task %q: %w", taskName, err)
	}

	fmt.Println("Task submitted:", taskName)

	return newTask, nil
}

func (s *Service) UpdateTaskStatus(id int64, status string) error {
	if !IsValidStatus(status) {
		return fmt.Errorf("update task status: invalid status %q", status)
	}

	if err := s.storage.UpdateStatus(id, status); err != nil {
		return fmt.Errorf("update task %d status: %w", id, err)
	}

	return nil
}
