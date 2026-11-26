package task

import (
	"context"
	"errors"
	"testing"
)

type mockStorage struct {
	err               error
	savedTask         *Task
	saveCalls         int
	updateStatusCalls int
	getByIDCalls      int
	requestedTaskID   int64
	foundTask         Task
	updatedTaskID     int64
	updatedStatus     string
	completionErr     error
	completionCalls   int
	completionTaskID  int64
	completionContext context.Context
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

func (m *mockStorage) GetByID(id int64) (Task, error) {
	m.getByIDCalls++
	m.requestedTaskID = id
	return m.foundTask, m.err
}

func (m *mockStorage) UpdateStatus(id int64, status string) error {
	m.updateStatusCalls++
	m.updatedTaskID = id
	m.updatedStatus = status
	return m.err
}

func (m *mockStorage) CompleteTaskContext(ctx context.Context, id int64) error {
	m.completionCalls++
	m.completionTaskID = id
	m.completionContext = ctx
	return m.completionErr
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

func TestGetTaskByID(t *testing.T) {
	expectedTask := Task{
		ID:     125,
		Name:   "send-email",
		Status: StatusPending,
	}

	storage := &mockStorage{foundTask: expectedTask}
	service := NewService("Test API", storage)

	foundTask, err := service.GetTaskByID(125)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if foundTask.ID != expectedTask.ID {
		t.Errorf("expected ID %d, got %d", expectedTask.ID, foundTask.ID)
	}

	if foundTask.Name != expectedTask.Name {
		t.Errorf("expected name %s, got %s", expectedTask.Name, foundTask.Name)
	}

	if storage.getByIDCalls != 1 {
		t.Errorf("expected 1 GetByID call, got %d", storage.getByIDCalls)
	}

	if storage.requestedTaskID != 125 {
		t.Errorf("expected requested ID 125, got %d", storage.requestedTaskID)
	}
}

func TestGetTaskByIDStorageError(t *testing.T) {
	expectedErr := errors.New("database unavailable")
	storage := &mockStorage{err: expectedErr}
	service := NewService("Test API", storage)

	_, err := service.GetTaskByID(125)

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected wrapped storage error, got %v", err)
	}
}

func TestGetTaskByIDInvalidID(t *testing.T) {
	storage := &mockStorage{}
	service := NewService("Test API", storage)

	_, err := service.GetTaskByID(0)

	if err == nil {
		t.Fatal("expected error for invalid ID")
	}

	if storage.getByIDCalls != 0 {
		t.Errorf("expected storage not to be called, got %d calls", storage.getByIDCalls)
	}
}

func TestGetTaskByIDContextCanceled(t *testing.T) {
	storage := &mockStorage{}
	service := NewService("Test API", storage)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := service.GetTaskByIDContext(ctx, 125)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	if storage.getByIDCalls != 0 {
		t.Fatalf("expected storage not to be called, got %d calls", storage.getByIDCalls)
	}
}

func TestGetTaskByIDContextLegacyStorageFallback(t *testing.T) {
	expectedTask := Task{
		ID:     125,
		Name:   "send-email",
		Status: StatusPending,
	}

	storage := &mockStorage{foundTask: expectedTask}
	service := NewService("Test API", storage)

	foundTask, err := service.GetTaskByIDContext(context.Background(), 125)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if foundTask.ID != expectedTask.ID {
		t.Errorf("expected ID %d, got %d", expectedTask.ID, foundTask.ID)
	}

	if storage.getByIDCalls != 1 {
		t.Errorf("expected 1 GetByID call, got %d", storage.getByIDCalls)
	}
}

type legacyMockStorage struct {
	Storage
}

func TestCompleteTaskContext(t *testing.T) {
	storage := &mockStorage{}
	service := NewService("Test API", storage)
	ctx := context.Background()

	if err := service.CompleteTaskContext(ctx, 125); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if storage.completionCalls != 1 {
		t.Fatalf("expected 1 completion call, got %d", storage.completionCalls)
	}

	if storage.completionTaskID != 125 {
		t.Errorf("expected task ID 125, got %d", storage.completionTaskID)
	}

	if storage.completionContext != ctx {
		t.Error("expected the original context to be passed to storage")
	}
}

func TestCompleteTaskContextStorageError(t *testing.T) {
	expectedErr := errors.New("database unavailable")
	storage := &mockStorage{completionErr: expectedErr}
	service := NewService("Test API", storage)

	err := service.CompleteTaskContext(context.Background(), 125)

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected wrapped storage error, got %v", err)
	}

	if storage.completionCalls != 1 {
		t.Errorf("expected 1 completion call, got %d", storage.completionCalls)
	}
}

func TestCompleteTaskContextInvalidID(t *testing.T) {
	storage := &mockStorage{}
	service := NewService("Test API", storage)

	err := service.CompleteTaskContext(context.Background(), 0)
	if err == nil {
		t.Fatal("expected error for invalid ID")
	}

	if storage.completionCalls != 0 {
		t.Errorf("expected storage not to be called, got %d calls", storage.completionCalls)
	}
}

func TestCompleteTaskContextCanceled(t *testing.T) {
	storage := &mockStorage{}
	service := NewService("Test API", storage)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := service.CompleteTaskContext(ctx, 125)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	if storage.completionCalls != 0 {
		t.Errorf("expected storage not to be called, got %d calls", storage.completionCalls)
	}
}

func TestCompleteTaskContextLegacyStorage(t *testing.T) {
	storage := &legacyMockStorage{Storage: &mockStorage{}}
	service := NewService("Test API", storage)

	err := service.CompleteTaskContext(context.Background(), 125)
	if err == nil {
		t.Fatal("expected error when storage does not support atomic completion")
	}
}
