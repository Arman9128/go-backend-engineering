package storage

import (
	"fmt"

	"github.com/Arman9128/go-backend-engineering/internal/task"
)

type MemoryStorage struct {
	nextID int64
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		nextID: 1,
	}
}

func (m *MemoryStorage) Save(taskData *task.Task) error {
	taskData.ID = m.nextID
	m.nextID++

	fmt.Printf(
		"Task saved in memory: ID=%d, Name=%s, Status=%s\n",
		taskData.ID,
		taskData.Name,
		taskData.Status,
	)

	return nil
}
