package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Arman9128/go-backend-engineering/internal/config"
	"github.com/Arman9128/go-backend-engineering/internal/database"
	"github.com/Arman9128/go-backend-engineering/internal/idempotency"
	"github.com/Arman9128/go-backend-engineering/internal/queue"
	"github.com/redis/go-redis/v9"
)

type failingEventProcessor struct{}

func (p *failingEventProcessor) Process(
	ctx context.Context,
	message queue.Message,
) error {
	return errors.New("simulated processing failure")
}

func TestWorkerDeadLetterIntegration(t *testing.T) {
	if os.Getenv("REDIS_INTEGRATION") != "1" {
		t.Skip("set REDIS_INTEGRATION=1 to run Redis integration test")
	}

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	redisClient, err := queue.NewRedisClient(addr)
	if err != nil {
		t.Fatalf("connect to Redis: %v", err)
	}
	defer redisClient.Close()

	rawClient := redis.NewClient(&redis.Options{Addr: addr})
	defer rawClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	stream := "test:worker:events:" + suffix
	deadLetterStream := stream + ":dead"
	group := "test-worker-" + suffix
	consumer := "worker-integration"

	defer rawClient.Del(context.Background(), stream, deadLetterStream)

	if err := redisClient.CreateConsumerGroup(ctx, stream, group); err != nil {
		t.Fatalf("create consumer group: %v", err)
	}

	eventID := time.Now().UnixNano()
	payload := []byte(`{"id":123,"name":"worker-integration-test"}`)

	if _, err := redisClient.Publish(
		ctx,
		stream,
		eventID,
		"task.created",
		payload,
	); err != nil {
		t.Fatalf("publish event: %v", err)
	}

	worker := NewWorker(
		redisClient,
		&failingEventProcessor{},
		stream,
		group,
		consumer,
	)
	worker.maxAttempts = 3
	worker.claimMinIdle = 0

	messages, err := redisClient.ReadGroup(
		ctx,
		stream,
		group,
		consumer,
		10,
		time.Second,
	)
	if err != nil {
		t.Fatalf("read initial message: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("initial messages = %d, want 1", len(messages))
	}

	message := messages[0]

	for attempt := int64(1); attempt <= worker.maxAttempts; attempt++ {
		if attempt > 1 {
			claimed, _, err := redisClient.ClaimPending(
				ctx,
				stream,
				group,
				consumer,
				0,
				"0-0",
				10,
			)
			if err != nil {
				t.Fatalf("claim pending message on attempt %d: %v", attempt, err)
			}

			if len(claimed) != 1 {
				t.Fatalf(
					"claimed messages on attempt %d = %d, want 1",
					attempt,
					len(claimed),
				)
			}

			message = claimed[0]
		}

		worker.processMessages(ctx, []queue.Message{message})

		if attempt < worker.maxAttempts {
			attempts, err := redisClient.GetDeliveryCount(
				ctx,
				stream,
				group,
				message.ID,
			)
			if err != nil {
				t.Fatalf("get delivery count on attempt %d: %v", attempt, err)
			}

			if attempts != attempt {
				t.Fatalf("delivery count = %d, want %d", attempts, attempt)
			}
		}
	}

	pending, err := rawClient.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  group,
		Start:  message.ID,
		End:    message.ID,
		Count:  1,
	}).Result()
	if err != nil {
		t.Fatalf("check pending messages: %v", err)
	}

	if len(pending) != 0 {
		t.Fatalf("original message still pending after DLQ move: %v", pending)
	}

	deadMessages, err := rawClient.XRange(ctx, deadLetterStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("read dead letter stream: %v", err)
	}

	if len(deadMessages) != 1 {
		t.Fatalf("dead-letter messages = %d, want 1", len(deadMessages))
	}

	values := deadMessages[0].Values

	expected := map[string]string{
		"original_message_id": message.ID,
		"event_id":            strconv.FormatInt(eventID, 10),
		"event_type":          "task.created",
		"payload":             string(payload),
		"attempts":            fmt.Sprintf("%d", worker.maxAttempts),
		"reason":              "simulated processing failure",
	}

	for key, want := range expected {
		got, ok := values[key]
		if !ok {
			t.Errorf("dead letter field %q is missing", key)
			continue
		}

		if got != want {
			t.Errorf("dead letter field %q = %v, want %q", key, got, want)
		}
	}
}

type countingEventProcessor struct {
	calls int
}

func (p *countingEventProcessor) Process(
	ctx context.Context,
	message queue.Message,
) error {
	p.calls++
	return nil
}

func TestWorkerIdempotencyRedisPostgresIntegration(t *testing.T) {
	if os.Getenv("REDIS_INTEGRATION") != "1" {
		t.Skip("set REDIS_INTEGRATION=1 to run Redis integration test")
	}

	if os.Getenv("POSTGRES_INTEGRATION") != "1" {
		t.Skip("set POSTGRES_INTEGRATION=1 to run PostgreSQL integration test")
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	db, err := database.NewPostgresDB(cfg)
	if err != nil {
		t.Fatalf("connect to PostgreSQL: %v", err)
	}
	defer db.Close()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	redisClient, err := queue.NewRedisClient(addr)
	if err != nil {
		t.Fatalf("connect to Redis: %v", err)
	}
	defer redisClient.Close()

	rawClient := redis.NewClient(&redis.Options{Addr: addr})
	defer rawClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	stream := "test:worker:idempotency:" + suffix
	group := "test-worker-idempotency-" + suffix
	consumer := "worker-idempotency-integration"
	eventID := time.Now().UnixNano()

	defer func() {
		if err := rawClient.Del(context.Background(), stream).Err(); err != nil {
			t.Errorf("delete test stream: %v", err)
		}
		if _, err := db.ExecContext(
			context.Background(),
			"DELETE FROM processed_events WHERE event_id = $1",
			eventID,
		); err != nil {
			t.Errorf("delete processed event: %v", err)
		}
	}()

	if err := redisClient.CreateConsumerGroup(ctx, stream, group); err != nil {
		t.Fatalf("create consumer group: %v", err)
	}

	store := idempotency.NewPostgresStore(db, 2*time.Minute)
	processor := &countingEventProcessor{}

	w := NewWorker(
		redisClient,
		processor,
		stream,
		group,
		consumer,
	)
	w.SetIdempotencyStore(store)

	payload := []byte(`{"id":123,"name":"idempotency-integration-test"}`)

	if _, err := redisClient.Publish(ctx, stream, eventID, "task.created", payload); err != nil {
		t.Fatalf("publish first event: %v", err)
	}

	firstMessages, err := redisClient.ReadGroup(
		ctx, stream, group, consumer, 10, time.Second,
	)
	if err != nil {
		t.Fatalf("read first event: %v", err)
	}
	if len(firstMessages) != 1 {
		t.Fatalf("first messages = %d, want 1", len(firstMessages))
	}

	w.processMessages(ctx, firstMessages)

	if processor.calls != 1 {
		t.Fatalf("processor calls after first delivery = %d, want 1", processor.calls)
	}

	var status string
	if err := db.QueryRowContext(
		ctx,
		"SELECT status FROM processed_events WHERE event_id = $1",
		eventID,
	).Scan(&status); err != nil {
		t.Fatalf("read processed event status: %v", err)
	}
	if status != "completed" {
		t.Fatalf("event status = %q, want completed", status)
	}

	if _, err := redisClient.Publish(ctx, stream, eventID, "task.created", payload); err != nil {
		t.Fatalf("publish duplicate event: %v", err)
	}

	duplicateMessages, err := redisClient.ReadGroup(
		ctx, stream, group, consumer, 10, time.Second,
	)
	if err != nil {
		t.Fatalf("read duplicate event: %v", err)
	}
	if len(duplicateMessages) != 1 {
		t.Fatalf("duplicate messages = %d, want 1", len(duplicateMessages))
	}

	w.processMessages(ctx, duplicateMessages)

	if processor.calls != 1 {
		t.Fatalf(
			"processor calls after duplicate delivery = %d, want 1",
			processor.calls,
		)
	}

	pending, err := rawClient.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  group,
		Start:  "-",
		End:    "+",
		Count:  10,
	}).Result()
	if err != nil {
		t.Fatalf("check pending messages: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending messages = %d, want 0", len(pending))
	}
}
