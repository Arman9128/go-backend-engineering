package task

import (
	"errors"
	"testing"
)

type mockStorage struct {
	err       error
	savedTask *Task
	saveCalls int
}

func (m *mockStorage) Save(task *Task) error {
	m.saveCalls++

	if m.err != nil {
		return m.err
	}

	task.ID = 125
	m.savedTask = task

	return nil
}

func TestSubmitTask(t *testing.T) {
	storage := &mockStorage{}
	service := NewService("Test API", storage)

	createdTask, err := service.SubmitTask("send-email")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if createdTask == nil {
		t.Fatal("expected created task, got nil")
	}

	if createdTask.ID != 125 {
		t.Errorf("expected ID 125, got %d", createdTask.ID)
	}

	if createdTask.Name != "send-email" {
		t.Errorf("expected send-email, got %s", createdTask.Name)
	}

	if createdTask.Status != StatusPending {
		t.Errorf("expected status %s, got %s", StatusPending, createdTask.Status)
	}

	if createdTask.CreatedAt.IsZero() {
		t.Error("expected CreatedAt to be initialized")
	}

	if createdTask != storage.savedTask {
		t.Error("expected returned task to be the same object saved in storage")
	}

	if storage.saveCalls != 1 {
		t.Errorf("expected 1 save call, got %d", storage.saveCalls)
	}
}

func TestSubmitTaskWithStorageError(t *testing.T) {
	expectedErr := errors.New("database connection failed")
	storage := &mockStorage{err: expectedErr}
	service := NewService("Test API", storage)

	createdTask, err := service.SubmitTask("send-email")

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected wrapped database error, got %v", err)
	}

	if createdTask != nil {
		t.Error("expected nil task when storage fails")
	}
}

func TestSubmitTaskWithEmptyName(t *testing.T) {
	storage := &mockStorage{}
	service := NewService("Test API", storage)

	createdTask, err := service.SubmitTask("")

	if err == nil {
		t.Fatal("expected error for empty task name")
	}

	if createdTask != nil {
		t.Error("expected nil task for empty name")
	}

	if storage.saveCalls != 0 {
		t.Errorf("expected storage not to be called, got %d calls", storage.saveCalls)
	}
}
