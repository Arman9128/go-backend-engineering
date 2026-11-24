package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

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
