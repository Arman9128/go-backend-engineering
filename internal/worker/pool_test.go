package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Arman9128/go-backend-engineering/internal/queue"
)

type poolTestQueue struct {
	mu        sync.Mutex
	consumers map[string]bool
	started   chan string
}

func newPoolTestQueue(workerCount int) *poolTestQueue {
	return &poolTestQueue{
		consumers: make(map[string]bool),
		started:   make(chan string, workerCount),
	}
}

func (q *poolTestQueue) CreateConsumerGroup(
	ctx context.Context,
	stream string,
	group string,
) error {
	return nil
}

func (q *poolTestQueue) ReadGroup(
	ctx context.Context,
	stream string,
	group string,
	consumer string,
	count int64,
	block time.Duration,
) ([]queue.Message, error) {
	q.mu.Lock()
	if !q.consumers[consumer] {
		q.consumers[consumer] = true
		q.started <- consumer
	}
	q.mu.Unlock()

	<-ctx.Done()
	return nil, ctx.Err()
}

func (q *poolTestQueue) Ack(
	ctx context.Context,
	stream string,
	group string,
	messageID string,
) error {
	return nil
}

func (q *poolTestQueue) MoveToDeadLetter(
	ctx context.Context,
	stream string,
	group string,
	deadLetterStream string,
	message queue.Message,
	maxAttempts int64,
	reason string,
) (bool, error) {
	return false, nil
}

func (q *poolTestQueue) ClaimPending(
	ctx context.Context,
	stream string,
	group string,
	consumer string,
	minIdle time.Duration,
	start string,
	count int64,
) ([]queue.Message, string, error) {
	return nil, "0-0", nil
}

type poolTestProcessor struct{}

func (p *poolTestProcessor) Process(
	ctx context.Context,
	message queue.Message,
) error {
	return nil
}

func TestPoolStartsMultipleConsumersAndStopsGracefully(t *testing.T) {
	const workerCount = 3

	messageQueue := newPoolTestQueue(workerCount)
	processor := &poolTestProcessor{}

	pool := NewPool(
		messageQueue,
		processor,
		"tasks:events",
		"task-workers",
		"worker",
		workerCount,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)

	go func() {
		done <- pool.Run(ctx)
	}()

	consumers := make(map[string]bool)

	for i := 0; i < workerCount; i++ {
		select {
		case consumer := <-messageQueue.started:
			if consumers[consumer] {
				t.Fatalf("duplicate consumer: %s", consumer)
			}
			consumers[consumer] = true

		case <-time.After(3 * time.Second):
			cancel()
			t.Fatal("timed out waiting for workers to start")
		}
	}

	expected := []string{"worker-1", "worker-2", "worker-3"}

	for _, consumer := range expected {
		if !consumers[consumer] {
			t.Errorf("consumer %q was not started", consumer)
		}
	}

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Pool.Run() error = %v, want context.Canceled", err)
		}

	case <-time.After(3 * time.Second):
		t.Fatal("worker pool did not stop after context cancellation")
	}

	messageQueue.mu.Lock()
	defer messageQueue.mu.Unlock()

	if len(messageQueue.consumers) != workerCount {
		t.Fatalf(
			"started consumers = %d, want %d",
			len(messageQueue.consumers),
			workerCount,
		)
	}
}

func TestPoolRejectsInvalidWorkerCount(t *testing.T) {
	pool := NewPool(
		newPoolTestQueue(1),
		&poolTestProcessor{},
		"tasks:events",
		"task-workers",
		"worker",
		0,
	)

	err := pool.Run(context.Background())

	if err == nil {
		t.Fatal("Pool.Run() error = nil, want validation error")
	}

	if err.Error() != "worker pool: worker count must be greater than zero" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPoolRejectsMissingConfiguration(t *testing.T) {
	tests := []struct {
		name string
		pool *Pool
	}{
		{
			name: "nil queue",
			pool: NewPool(nil, &poolTestProcessor{}, "stream", "group", "worker", 1),
		},
		{
			name: "nil processor",
			pool: NewPool(newPoolTestQueue(1), nil, "stream", "group", "worker", 1),
		},
		{
			name: "missing stream",
			pool: NewPool(newPoolTestQueue(1), &poolTestProcessor{}, "", "group", "worker", 1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.pool.Run(context.Background()); err == nil {
				t.Fatal("Pool.Run() error = nil, want validation error")
			}
		})
	}
}

func TestPoolReturnsWorkerStartupError(t *testing.T) {
	expectedErr := errors.New("Redis unavailable")

	messageQueue := newPoolTestQueue(1)
	messageQueue.mu.Lock()
	messageQueue.consumers["worker-1"] = true
	messageQueue.mu.Unlock()

	failingQueue := &poolStartupErrorQueue{
		poolTestQueue: messageQueue,
		err:           expectedErr,
	}

	pool := NewPool(
		failingQueue,
		&poolTestProcessor{},
		"tasks:events",
		"task-workers",
		"worker",
		1,
	)

	err := pool.Run(context.Background())

	if !errors.Is(err, expectedErr) {
		t.Fatalf("Pool.Run() error = %v, want wrapped startup error", err)
	}
}

type poolStartupErrorQueue struct {
	*poolTestQueue
	err error
}

func (q *poolStartupErrorQueue) CreateConsumerGroup(
	ctx context.Context,
	stream string,
	group string,
) error {
	return fmt.Errorf("create consumer group: %w", q.err)
}
