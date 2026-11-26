package storage

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/Arman9128/go-backend-engineering/internal/task"
)

func TestSaveAndGetByID(t *testing.T) {
	storage := NewMemoryStorage()

	newTask := &task.Task{
		Name:   "send-email",
		Status: task.StatusPending,
	}

	if err := storage.Save(newTask); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	savedTask, err := storage.GetByID(newTask.ID)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if savedTask.Name != "send-email" {
		t.Errorf("expected send-email, got %s", savedTask.Name)
	}
}

func TestGetByIDNotFound(t *testing.T) {
	storage := NewMemoryStorage()

	foundTask, err := storage.GetByID(999)

	if err == nil {
		t.Fatal("expected error for missing task")
	}

	if foundTask.ID != 0 || foundTask.Name != "" || foundTask.Status != "" || !foundTask.CreatedAt.IsZero() || foundTask.Tags != nil {
		t.Error("expected zero-value task")
	}
}

func TestConcurrentSave(t *testing.T) {
	storage := NewMemoryStorage()

	const count = 100

	var wg sync.WaitGroup

	for i := 0; i < count; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			newTask := &task.Task{
				Name:   "background-task",
				Status: task.StatusPending,
			}

			if err := storage.Save(newTask); err != nil {
				t.Errorf("unexpected save error: %v", err)
			}
		}()
	}

	wg.Wait()

	storage.mu.RLock()
	defer storage.mu.RUnlock()

	if len(storage.tasks) != count {
		t.Errorf("expected %d tasks, got %d", count, len(storage.tasks))
	}
}

func TestConcurrentSaveAndGetByID(t *testing.T) {
	storage := NewMemoryStorage()

	// Create a task that readers will access.
	initialTask := &task.Task{
		Name:   "initial-task",
		Status: task.StatusPending,
	}

	if err := storage.Save(initialTask); err != nil {
		t.Fatalf("failed to save initial task: %v", err)
	}

	const (
		readerCount    = 10
		writerCount    = 100
		readsPerWorker = 1000
	)

	var wg sync.WaitGroup
	start := make(chan struct{})

	// Start concurrent readers.
	for i := 0; i < readerCount; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			<-start

			for j := 0; j < readsPerWorker; j++ {
				foundTask, err := storage.GetByID(initialTask.ID)
				if err != nil {
					t.Errorf("unexpected read error: %v", err)
					return
				}

				if foundTask.Name != "initial-task" {
					t.Errorf("expected initial-task, got %s", foundTask.Name)
					return
				}
			}
		}()
	}

	// Start concurrent writers.
	for i := 0; i < writerCount; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			<-start

			newTask := &task.Task{
				Name:   "background-task",
				Status: task.StatusPending,
			}

			if err := storage.Save(newTask); err != nil {
				t.Errorf("unexpected save error: %v", err)
			}
		}()
	}

	// Release all goroutines simultaneously.
	close(start)

	wg.Wait()
}

func TestSaveStoresIndependentCopy(t *testing.T) {
	storage := NewMemoryStorage()

	newTask := &task.Task{
		Name:   "send-email",
		Status: task.StatusPending,
	}

	if err := storage.Save(newTask); err != nil {
		t.Fatalf("unexpected save error: %v", err)
	}

	newTask.Name = "modified-task"

	savedTask, err := storage.GetByID(newTask.ID)
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}

	if savedTask.Name != "send-email" {
		t.Errorf("expected send-email, got %s", savedTask.Name)
	}
}

func TestSaveStoresIndependentTags(t *testing.T) {
	storage := NewMemoryStorage()

	newTask := &task.Task{
		Name:   "send-email",
		Status: task.StatusPending,
		Tags:   []string{"email", "notification"},
	}

	if err := storage.Save(newTask); err != nil {
		t.Fatalf("unexpected save error: %v", err)
	}

	newTask.Tags[0] = "modified"

	savedTask, err := storage.GetByID(newTask.ID)
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}

	if savedTask.Tags[0] != "email" {
		t.Errorf("expected email, got %s", savedTask.Tags[0])
	}
}

func TestGetByIDReturnsIndependentTags(t *testing.T) {
	storage := NewMemoryStorage()

	newTask := &task.Task{
		Name:   "send-email",
		Status: task.StatusPending,
		Tags:   []string{"email", "notification"},
	}

	if err := storage.Save(newTask); err != nil {
		t.Fatalf("unexpected save error: %v", err)
	}

	foundTask, err := storage.GetByID(newTask.ID)
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}

	foundTask.Tags[0] = "modified"

	storedTask, err := storage.GetByID(newTask.ID)
	if err != nil {
		t.Fatalf("unexpected second get error: %v", err)
	}

	if storedTask.Tags[0] != "email" {
		t.Errorf("expected email, got %s", storedTask.Tags[0])
	}
}

func TestUpdateStatus(t *testing.T) {
	storage := NewMemoryStorage()

	newTask := &task.Task{
		Name:   "send-email",
		Status: task.StatusPending,
	}

	if err := storage.Save(newTask); err != nil {
		t.Fatalf("unexpected save error: %v", err)
	}

	if err := storage.UpdateStatus(newTask.ID, task.StatusProcessing); err != nil {
		t.Fatalf("unexpected update error: %v", err)
	}

	updatedTask, err := storage.GetByID(newTask.ID)
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}

	if updatedTask.Status != task.StatusProcessing {
		t.Errorf("expected status %s, got %s",
			task.StatusProcessing, updatedTask.Status)
	}
}

func TestUpdateStatusNotFound(t *testing.T) {
	storage := NewMemoryStorage()

	err := storage.UpdateStatus(999, task.StatusProcessing)

	if err == nil {
		t.Fatal("expected error for missing task")
	}
}

func TestUpdateStatusInvalidStatus(t *testing.T) {
	storage := NewMemoryStorage()

	newTask := &task.Task{
		Name:   "send-email",
		Status: task.StatusPending,
	}

	if err := storage.Save(newTask); err != nil {
		t.Fatalf("unexpected save error: %v", err)
	}

	err := storage.UpdateStatus(newTask.ID, "unknown")

	if err == nil {
		t.Fatal("expected error for invalid status")
	}

	storedTask, err := storage.GetByID(newTask.ID)
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}

	if storedTask.Status != task.StatusPending {
		t.Errorf("expected status to remain pending, got %s", storedTask.Status)
	}
}

func TestMemoryStorageCompleteTaskContext(t *testing.T) {
	storage := NewMemoryStorage()

	newTask := &task.Task{
		Name:   "send-email",
		Status: task.StatusPending,
	}

	if err := storage.Save(newTask); err != nil {
		t.Fatalf("unexpected save error: %v", err)
	}

	ctx := context.Background()

	if err := storage.CompleteTaskContext(ctx, newTask.ID); err != nil {
		t.Fatalf("unexpected completion error: %v", err)
	}

	completedTask, err := storage.GetByID(newTask.ID)
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}

	if completedTask.Status != task.StatusCompleted {
		t.Fatalf("status = %q, want %q", completedTask.Status, task.StatusCompleted)
	}

	if err := storage.CompleteTaskContext(ctx, newTask.ID); err != nil {
		t.Fatalf("repeated completion should succeed, got: %v", err)
	}
}

func TestMemoryStorageCompleteTaskContextInvalidTransition(t *testing.T) {
	storage := NewMemoryStorage()

	newTask := &task.Task{
		Name:   "send-email",
		Status: task.StatusProcessing,
	}

	if err := storage.Save(newTask); err != nil {
		t.Fatalf("unexpected save error: %v", err)
	}

	err := storage.CompleteTaskContext(context.Background(), newTask.ID)
	if !errors.Is(err, task.ErrInvalidTransition) {
		t.Fatalf("error = %v, want %v", err, task.ErrInvalidTransition)
	}
}

func TestMemoryStorageCompleteTaskContextNotFound(t *testing.T) {
	storage := NewMemoryStorage()

	err := storage.CompleteTaskContext(context.Background(), 999)
	if !errors.Is(err, task.ErrNotFound) {
		t.Fatalf("error = %v, want %v", err, task.ErrNotFound)
	}
}

func TestMemoryStorageCompleteTaskContextCanceled(t *testing.T) {
	storage := NewMemoryStorage()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := storage.CompleteTaskContext(ctx, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want %v", err, context.Canceled)
	}
}
