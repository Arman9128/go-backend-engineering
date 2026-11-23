package task

import (
	"errors"
	"testing"
)

type mockStorage struct {
	err               error
	savedTask         *Task
	saveCalls         int
	updateStatusCalls int
	updatedTaskID     int64
	updatedStatus     string
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

func (m *mockStorage) UpdateStatus(id int64, status string) error {
	m.updateStatusCalls++
	m.updatedTaskID = id
	m.updatedStatus = status
	return m.err
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

func TestUpdateTaskStatus(t *testing.T) {
	storage := &mockStorage{}
	service := NewService("Test API", storage)

	err := service.UpdateTaskStatus(125, StatusProcessing)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if storage.updateStatusCalls != 1 {
		t.Errorf("expected 1 update call, got %d", storage.updateStatusCalls)
	}

	if storage.updatedTaskID != 125 {
		t.Errorf("expected task ID 125, got %d", storage.updatedTaskID)
	}

	if storage.updatedStatus != StatusProcessing {
		t.Errorf("expected status %s, got %s", StatusProcessing, storage.updatedStatus)
	}
}

func TestUpdateTaskStatusInvalidStatus(t *testing.T) {
	storage := &mockStorage{}
	service := NewService("Test API", storage)

	err := service.UpdateTaskStatus(125, "unknown")
	if err == nil {
		t.Fatal("expected error for invalid status")
	}

	if storage.updateStatusCalls != 0 {
		t.Errorf("expected storage not to be called, got %d calls", storage.updateStatusCalls)
	}
}

func TestUpdateTaskStatusStorageError(t *testing.T) {
	storageErr := errors.New("database unavailable")
	storage := &mockStorage{err: storageErr}
	service := NewService("Test API", storage)

	err := service.UpdateTaskStatus(125, StatusProcessing)
	if !errors.Is(err, storageErr) {
		t.Errorf("expected wrapped storage error, got %v", err)
	}
}
