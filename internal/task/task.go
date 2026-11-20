package task

import "fmt"

type Storage interface {
	Save(taskName string) error
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

func (s *Service) SubmitTask(taskName string) error {
	if taskName == "" {
		return fmt.Errorf("task name cannot be empty")
	}

	if err := s.storage.Save(taskName); err != nil {
		return fmt.Errorf("submit task %q: %w", taskName, err)
	}

	fmt.Println("Task submitted:", taskName)

	return nil
}
