package task

import (
	"errors"
	"testing"
)

type mockStorage struct {
	err       error
	savedTask string
	saveCalls int
}

func (m *mockStorage) Save(taskName string) error {
	m.saveCalls++
	m.savedTask = taskName
	return m.err
}

func TestSubmitTask(t *testing.T) {
	storage := &mockStorage{}
	service := NewService("Test API", storage)

	err := service.SubmitTask("send-email")

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if storage.savedTask != "send-email" {
		t.Errorf("expected send-email, got %s", storage.savedTask)
	}

	if storage.saveCalls != 1 {
		t.Errorf("expected 1 save call, got %d", storage.saveCalls)
	}
}

func TestSubmitTaskWithStorageError(t *testing.T) {
	expectedErr := errors.New("database connection failed")

	storage := &mockStorage{
		err: expectedErr,
	}

	service := NewService("Test API", storage)

	err := service.SubmitTask("send-email")

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected wrapped database error, got %v", err)
	}
}

func TestSubmitTaskWithEmptyName(t *testing.T) {
	storage := &mockStorage{}
	service := NewService("Test API", storage)

	err := service.SubmitTask("")

	if err == nil {
		t.Fatal("expected error for empty task name")
	}

	if storage.saveCalls != 0 {
		t.Errorf("expected storage not to be called, got %d calls", storage.saveCalls)
	}
}
