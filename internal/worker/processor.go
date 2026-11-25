package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/Arman9128/go-backend-engineering/internal/queue"
	"github.com/Arman9128/go-backend-engineering/internal/task"
)

type EventProcessor interface {
	Process(ctx context.Context, message queue.Message) error
}

type TaskService interface {
	GetTaskByID(id int64) (task.Task, error)
}

type ContextTaskService interface {
	GetTaskByIDContext(ctx context.Context, id int64) (task.Task, error)
}

type TaskProcessor struct {
	taskService TaskService
}

func NewTaskProcessor(taskService TaskService) *TaskProcessor {
	return &TaskProcessor{
		taskService: taskService,
	}
}

type TaskCreatedEvent struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	Tags      []string  `json:"tags"`
}

func (p *TaskProcessor) Process(ctx context.Context, message queue.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if message.EventType != "task.created" {
		return fmt.Errorf("unsupported event type %q", message.EventType)
	}

	if message.EventID <= 0 {
		return fmt.Errorf("invalid event ID %d", message.EventID)
	}

	var event TaskCreatedEvent
	if err := json.Unmarshal(message.Payload, &event); err != nil {
		return fmt.Errorf("decode task.created event: %w", err)
	}

	if event.ID <= 0 {
		return fmt.Errorf("invalid task ID %d", event.ID)
	}

	if event.Name == "" {
		return fmt.Errorf("task name cannot be empty")
	}

	if event.CreatedAt.IsZero() {
		return fmt.Errorf("task creation time cannot be zero")
	}

	var foundTask task.Task
	var err error

	if service, ok := p.taskService.(ContextTaskService); ok {
		foundTask, err = service.GetTaskByIDContext(ctx, event.ID)
	} else {
		if err := ctx.Err(); err != nil {
			return err
		}
		foundTask, err = p.taskService.GetTaskByID(event.ID)
	}
	if err != nil {
		return fmt.Errorf("get task %d for event %d: %w", event.ID, message.EventID, err)
	}

	if foundTask.ID != event.ID ||
		foundTask.Name != event.Name ||
		!foundTask.CreatedAt.Equal(event.CreatedAt) ||
		!slices.Equal(foundTask.Tags, event.Tags) {
		return fmt.Errorf("task data does not match event %d", message.EventID)
	}

	return nil
}
