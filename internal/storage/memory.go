package storage

import (
	"context"
	"fmt"
	"sync"

	"github.com/Arman9128/go-backend-engineering/internal/task"
)

type MemoryStorage struct {
	mu     sync.RWMutex
	tasks  map[int64]*task.Task
	nextID int64
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		tasks:  make(map[int64]*task.Task),
		nextID: 1,
	}
}

func (m *MemoryStorage) Save(taskData *task.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	taskData.ID = m.nextID
	m.nextID++

	taskCopy := *taskData
	taskCopy.Tags = make([]string, len(taskData.Tags))
	copy(taskCopy.Tags, taskData.Tags)
	m.tasks[taskData.ID] = &taskCopy

	fmt.Printf(
		"Task saved in memory: ID=%d, Name=%s, Status=%s\n",
		taskData.ID,
		taskData.Name,
		taskData.Status,
	)

	return nil
}

func (m *MemoryStorage) GetByID(id int64) (task.Task, error) {
	return m.GetByIDContext(context.Background(), id)
}

func (m *MemoryStorage) GetByIDContext(ctx context.Context, id int64) (task.Task, error) {
	if err := ctx.Err(); err != nil {
		return task.Task{}, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	taskData, exists := m.tasks[id]
	if !exists {
		return task.Task{}, fmt.Errorf("get task %d: %w", id, task.ErrNotFound)
	}

	taskCopy := *taskData

	if taskData.Tags != nil {
		taskCopy.Tags = make([]string, len(taskData.Tags))
		copy(taskCopy.Tags, taskData.Tags)
	}

	return taskCopy, nil
}

func (m *MemoryStorage) UpdateStatus(id int64, status string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	taskData, exists := m.tasks[id]
	if !exists {
		return fmt.Errorf("update task %d: %w", id, task.ErrNotFound)
	}

	if !task.IsValidStatus(status) {
		return fmt.Errorf("invalid task status: %s", status)
	}

	taskData.Status = status

	return nil
}

func (m *MemoryStorage) CompleteTaskContext(ctx context.Context, id int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	taskData, exists := m.tasks[id]
	if !exists {
		return fmt.Errorf("complete task %d: %w", id, task.ErrNotFound)
	}

	if taskData.Status == task.StatusCompleted {
		return nil
	}

	if taskData.Status != task.StatusPending {
		return fmt.Errorf(
			"complete task %d from status %q: %w",
			id,
			taskData.Status,
			task.ErrInvalidTransition,
		)
	}

	taskData.Status = task.StatusCompleted

	return nil
}
