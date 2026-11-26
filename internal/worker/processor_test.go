package worker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Arman9128/go-backend-engineering/internal/queue"
	"github.com/Arman9128/go-backend-engineering/internal/task"
)

type mockTaskService struct {
	task            task.Task
	err             error
	id              int64
	calls           int
	completionErr   error
	completionID    int64
	completionCalls int
	completionCtx   context.Context
}

func (m *mockTaskService) GetTaskByID(id int64) (task.Task, error) {
	m.id = id
	m.calls++
	return m.task, m.err
}

func (m *mockTaskService) CompleteTaskContext(ctx context.Context, id int64) error {
	m.completionCtx = ctx
	m.completionID = id
	m.completionCalls++
	return m.completionErr
}

type contextAwareMockTaskService struct {
	mockTaskService
	receivedCtx context.Context
}

func (m *contextAwareMockTaskService) GetTaskByIDContext(ctx context.Context, id int64) (task.Task, error) {
	m.receivedCtx = ctx
	m.id = id
	m.calls++
	return m.task, m.err
}

func TestTaskProcessorPassesContextToTaskService(t *testing.T) {
	createdAt := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

	event := TaskCreatedEvent{
		ID:        10,
		Name:      "test-task",
		Status:    task.StatusPending,
		CreatedAt: createdAt,
	}

	service := &contextAwareMockTaskService{
		mockTaskService: mockTaskService{
			task: task.Task{
				ID:        event.ID,
				Name:      event.Name,
				Status:    event.Status,
				CreatedAt: event.CreatedAt,
			},
		},
	}

	processor := NewTaskProcessor(service)

	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.WithValue(context.Background(), struct{}{}, "test-context")
	message := queue.Message{
		ID:        "1-0",
		EventID:   1,
		EventType: "task.created",
		Payload:   payload,
	}

	if err := processor.Process(ctx, message); err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if service.receivedCtx != ctx {
		t.Fatal("TaskProcessor did not pass the original context to TaskService")
	}

	if service.calls != 1 {
		t.Fatalf("GetTaskByIDContext() calls = %d, want 1", service.calls)
	}

	if service.id != event.ID {
		t.Fatalf("GetTaskByIDContext() ID = %d, want %d", service.id, event.ID)
	}

	if service.completionCalls != 1 {
		t.Fatalf("CompleteTaskContext() calls = %d, want 1", service.completionCalls)
	}

	if service.completionID != event.ID {
		t.Fatalf("CompleteTaskContext() ID = %d, want %d", service.completionID, event.ID)
	}

	if service.completionCtx != ctx {
		t.Fatal("TaskProcessor did not pass the original context to CompleteTaskContext")
	}
}

func TestTaskProcessorProcessSuccess(t *testing.T) {
	createdAt := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

	event := TaskCreatedEvent{
		ID:        10,
		Name:      "test-task",
		Status:    task.StatusPending,
		CreatedAt: createdAt,
		Tags:      []string{"go", "redis"},
	}

	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}

	service := &mockTaskService{
		task: task.Task{
			ID:        event.ID,
			Name:      event.Name,
			Status:    task.StatusPending,
			CreatedAt: event.CreatedAt,
			Tags:      event.Tags,
		},
	}

	processor := NewTaskProcessor(service)

	message := queue.Message{
		ID:        "1-0",
		EventID:   1,
		EventType: "task.created",
		Payload:   payload,
	}

	ctx := context.Background()
	if err := processor.Process(ctx, message); err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if service.calls != 1 {
		t.Fatalf("GetTaskByID() calls = %d, want 1", service.calls)
	}

	if service.id != event.ID {
		t.Fatalf("GetTaskByID() ID = %d, want %d", service.id, event.ID)
	}

	if service.completionCalls != 1 {
		t.Fatalf("CompleteTaskContext() calls = %d, want 1", service.completionCalls)
	}

	if service.completionID != event.ID {
		t.Fatalf("CompleteTaskContext() ID = %d, want %d", service.completionID, event.ID)
	}

	if service.completionCtx != ctx {
		t.Fatal("TaskProcessor did not pass the original context to CompleteTaskContext")
	}
}

func TestTaskProcessorProcessInvalidEventType(t *testing.T) {
	service := &mockTaskService{}
	processor := NewTaskProcessor(service)

	message := queue.Message{
		EventID:   1,
		EventType: "task.updated",
	}

	err := processor.Process(context.Background(), message)
	if err == nil {
		t.Fatal("Process() error = nil, want error")
	}

	if service.calls != 0 {
		t.Fatalf("GetTaskByID() calls = %d, want 0", service.calls)
	}
}

func TestTaskProcessorProcessInvalidPayload(t *testing.T) {
	service := &mockTaskService{}
	processor := NewTaskProcessor(service)

	message := queue.Message{
		EventID:   1,
		EventType: "task.created",
		Payload:   []byte("{invalid json"),
	}

	err := processor.Process(context.Background(), message)
	if err == nil {
		t.Fatal("Process() error = nil, want error")
	}

	if service.calls != 0 {
		t.Fatalf("GetTaskByID() calls = %d, want 0", service.calls)
	}
}

func TestTaskProcessorProcessTaskServiceError(t *testing.T) {
	createdAt := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

	payload, err := json.Marshal(TaskCreatedEvent{
		ID:        10,
		Name:      "test-task",
		Status:    task.StatusPending,
		CreatedAt: createdAt,
	})
	if err != nil {
		t.Fatal(err)
	}

	serviceErr := errors.New("database unavailable")
	service := &mockTaskService{err: serviceErr}
	processor := NewTaskProcessor(service)

	message := queue.Message{
		EventID:   1,
		EventType: "task.created",
		Payload:   payload,
	}

	err = processor.Process(context.Background(), message)
	if !errors.Is(err, serviceErr) {
		t.Fatalf("Process() error = %v, want wrapped service error", err)
	}
}

func TestTaskProcessorProcessMismatchedTask(t *testing.T) {
	createdAt := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

	payload, err := json.Marshal(TaskCreatedEvent{
		ID:        10,
		Name:      "event-task",
		Status:    task.StatusPending,
		CreatedAt: createdAt,
	})
	if err != nil {
		t.Fatal(err)
	}

	service := &mockTaskService{
		task: task.Task{
			ID:        10,
			Name:      "different-task",
			CreatedAt: createdAt,
		},
	}

	processor := NewTaskProcessor(service)

	message := queue.Message{
		EventID:   1,
		EventType: "task.created",
		Payload:   payload,
	}

	if err := processor.Process(context.Background(), message); err == nil {
		t.Fatal("Process() error = nil, want mismatch error")
	}
}

func TestTaskProcessorProcessCompletionError(t *testing.T) {
	createdAt := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

	event := TaskCreatedEvent{
		ID:        10,
		Name:      "test-task",
		Status:    task.StatusPending,
		CreatedAt: createdAt,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}

	completionErr := errors.New("completion failed")
	service := &mockTaskService{
		task: task.Task{
			ID:        event.ID,
			Name:      event.Name,
			Status:    task.StatusPending,
			CreatedAt: event.CreatedAt,
		},
		completionErr: completionErr,
	}

	processor := NewTaskProcessor(service)

	message := queue.Message{
		ID:        "1-0",
		EventID:   1,
		EventType: "task.created",
		Payload:   payload,
	}

	err = processor.Process(context.Background(), message)
	if !errors.Is(err, completionErr) {
		t.Fatalf("Process() error = %v, want wrapped completion error", err)
	}

	if service.completionCalls != 1 {
		t.Fatalf("CompleteTaskContext() calls = %d, want 1", service.completionCalls)
	}

	if service.completionID != event.ID {
		t.Fatalf("CompleteTaskContext() ID = %d, want %d", service.completionID, event.ID)
	}
}

type legacyTaskService struct {
	task  task.Task
	calls int
}

func (m *legacyTaskService) GetTaskByID(id int64) (task.Task, error) {
	m.calls++
	return m.task, nil
}

func TestTaskProcessorProcessWithoutCompletionSupport(t *testing.T) {
	createdAt := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

	event := TaskCreatedEvent{
		ID:        10,
		Name:      "test-task",
		Status:    task.StatusPending,
		CreatedAt: createdAt,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}

	service := &legacyTaskService{
		task: task.Task{
			ID:        event.ID,
			Name:      event.Name,
			Status:    event.Status,
			CreatedAt: event.CreatedAt,
		},
	}

	processor := NewTaskProcessor(service)

	message := queue.Message{
		ID:        "1-0",
		EventID:   1,
		EventType: "task.created",
		Payload:   payload,
	}

	err = processor.Process(context.Background(), message)
	if err == nil {
		t.Fatal("Process() error = nil, want completion support error")
	}

	if service.calls != 1 {
		t.Fatalf("GetTaskByID() calls = %d, want 1", service.calls)
	}

	if !strings.Contains(err.Error(), "does not support completion") {
		t.Fatalf("Process() error = %v, want completion support error", err)
	}
}

func TestTaskProcessorProcessInvalidInitialStatus(t *testing.T) {
	createdAt := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

	event := TaskCreatedEvent{
		ID:        10,
		Name:      "test-task",
		Status:    task.StatusCompleted,
		CreatedAt: createdAt,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}

	service := &mockTaskService{}
	processor := NewTaskProcessor(service)

	message := queue.Message{
		ID:        "1-0",
		EventID:   1,
		EventType: "task.created",
		Payload:   payload,
	}

	err = processor.Process(context.Background(), message)
	if err == nil {
		t.Fatal("Process() error = nil, want invalid initial status error")
	}

	if !strings.Contains(err.Error(), "invalid initial status") {
		t.Fatalf("Process() error = %v, want invalid initial status error", err)
	}

	if service.calls != 0 {
		t.Fatalf("GetTaskByID() calls = %d, want 0", service.calls)
	}

	if service.completionCalls != 0 {
		t.Fatalf("CompleteTaskContext() calls = %d, want 0", service.completionCalls)
	}
}
