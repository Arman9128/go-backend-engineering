package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Arman9128/go-backend-engineering/internal/queue"
	"github.com/Arman9128/go-backend-engineering/internal/task"
)

type mockTaskService struct {
	task  task.Task
	err   error
	id    int64
	calls int
}

func (m *mockTaskService) GetTaskByID(id int64) (task.Task, error) {
	m.id = id
	m.calls++
	return m.task, m.err
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

	if err := processor.Process(context.Background(), message); err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if service.calls != 1 {
		t.Fatalf("GetTaskByID() calls = %d, want 1", service.calls)
	}

	if service.id != event.ID {
		t.Fatalf("GetTaskByID() ID = %d, want %d", service.id, event.ID)
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
