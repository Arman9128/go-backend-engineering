package storage

import "fmt"

type MemoryStorage struct{}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{}
}

func (m *MemoryStorage) Save(taskName string) error {
	fmt.Println("Task saved in memory:", taskName)
	return nil
}
