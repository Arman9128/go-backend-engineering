package queue

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisConnection(t *testing.T) {
	if os.Getenv("REDIS_INTEGRATION") != "1" {
		t.Skip("set REDIS_INTEGRATION=1 to run Redis integration test")
	}

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	redisClient, err := NewRedisClient(addr)
	if err != nil {
		t.Fatalf("failed to connect to Redis: %v", err)
	}
	defer redisClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := redisClient.client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Redis PING failed: %v", err)
	}
}

func TestRedisPublish(t *testing.T) {
	if os.Getenv("REDIS_INTEGRATION") != "1" {
		t.Skip("set REDIS_INTEGRATION=1 to run Redis integration test")
	}

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	redisClient, err := NewRedisClient(addr)
	if err != nil {
		t.Fatalf("connect to Redis: %v", err)
	}
	defer redisClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream := "test:outbox:events"
	eventID := int64(time.Now().UnixNano())
	eventType := "task.created"
	payload := []byte(`{"id":123,"name":"integration-test-task"}`)

	entryID, err := redisClient.Publish(ctx, stream, eventID, eventType, payload)
	if err != nil {
		t.Fatalf("publish event: %v", err)
	}

	if entryID == "" {
		t.Fatal("expected Redis Stream entry ID")
	}

	entries, err := redisClient.client.XRange(ctx, stream, entryID, entryID).Result()
	if err != nil {
		t.Fatalf("read Redis Stream entry: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected 1 stream entry, got %d", len(entries))
	}

	values := entries[0].Values

	gotEventID, ok := values["event_id"].(string)
	if !ok {
		t.Fatalf("event_id has unexpected type: %T", values["event_id"])
	}

	if gotEventID != strconv.FormatInt(eventID, 10) {
		t.Errorf("event_id = %q, want %d", gotEventID, eventID)
	}

	if got := values["event_type"]; got != eventType {
		t.Errorf("event_type = %v, want %q", got, eventType)
	}

	if got := values["payload"]; got != string(payload) {
		t.Errorf("payload = %v, want %s", got, payload)
	}

	if err := redisClient.client.XDel(ctx, stream, entryID).Err(); err != nil {
		t.Errorf("delete test stream entry: %v", err)
	}
}

func TestRedisConsumerGroup(t *testing.T) {
	if os.Getenv("REDIS_INTEGRATION") != "1" {
		t.Skip("set REDIS_INTEGRATION=1 to run Redis integration test")
	}

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	redisClient, err := NewRedisClient(addr)
	if err != nil {
		t.Fatalf("connect to Redis: %v", err)
	}
	defer redisClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	stream := "test:consumer:events:" + suffix
	group := "test-workers-" + suffix
	defer redisClient.client.Del(context.Background(), stream)

	eventID := time.Now().UnixNano()
	eventType := "task.created"
	payload := []byte(`{"id":123,"name":"consumer-test"}`)

	if _, err := redisClient.Publish(ctx, stream, eventID, eventType, payload); err != nil {
		t.Fatalf("publish event: %v", err)
	}

	if err := redisClient.CreateConsumerGroup(ctx, stream, group); err != nil {
		t.Fatalf("create consumer group: %v", err)
	}

	if err := redisClient.CreateConsumerGroup(ctx, stream, group); err != nil {
		t.Fatalf("create existing consumer group: %v", err)
	}

	messages, err := redisClient.ReadGroup(ctx, stream, group, "consumer-1", 10, time.Second)
	if err != nil {
		t.Fatalf("read consumer group: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	message := messages[0]

	if message.EventID != eventID {
		t.Errorf("event ID = %d, want %d", message.EventID, eventID)
	}

	if message.EventType != eventType {
		t.Errorf("event type = %q, want %q", message.EventType, eventType)
	}

	if string(message.Payload) != string(payload) {
		t.Errorf("payload = %s, want %s", message.Payload, payload)
	}

	if err := redisClient.Ack(ctx, stream, group, message.ID); err != nil {
		t.Fatalf("acknowledge message: %v", err)
	}
}

func TestRedisClaimPending(t *testing.T) {
	if os.Getenv("REDIS_INTEGRATION") != "1" {
		t.Skip("set REDIS_INTEGRATION=1 to run Redis integration test")
	}

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	redisClient, err := NewRedisClient(addr)
	if err != nil {
		t.Fatalf("connect to Redis: %v", err)
	}
	defer redisClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	stream := "test:claim:events:" + suffix
	group := "test-claim-workers-" + suffix
	defer redisClient.client.Del(context.Background(), stream)

	eventID := time.Now().UnixNano()
	eventType := "task.created"
	payload := []byte(`{"id":456,"name":"claim-test"}`)

	if _, err := redisClient.Publish(ctx, stream, eventID, eventType, payload); err != nil {
		t.Fatalf("publish event: %v", err)
	}

	if err := redisClient.CreateConsumerGroup(ctx, stream, group); err != nil {
		t.Fatalf("create consumer group: %v", err)
	}

	originalMessages, err := redisClient.ReadGroup(
		ctx, stream, group, "consumer-1", 10, time.Second,
	)
	if err != nil {
		t.Fatalf("read message with consumer-1: %v", err)
	}

	if len(originalMessages) != 1 {
		t.Fatalf("expected 1 original message, got %d", len(originalMessages))
	}

	time.Sleep(100 * time.Millisecond)

	claimedMessages, _, err := redisClient.ClaimPending(
		ctx, stream, group, "consumer-2", 50*time.Millisecond, "0-0", 10,
	)
	if err != nil {
		t.Fatalf("claim pending message: %v", err)
	}

	if len(claimedMessages) != 1 {
		t.Fatalf("expected 1 claimed message, got %d", len(claimedMessages))
	}

	claimed := claimedMessages[0]

	if claimed.ID != originalMessages[0].ID {
		t.Errorf("claimed message ID = %q, want %q", claimed.ID, originalMessages[0].ID)
	}

	if claimed.EventID != eventID {
		t.Errorf("claimed event ID = %d, want %d", claimed.EventID, eventID)
	}

	if err := redisClient.Ack(ctx, stream, group, claimed.ID); err != nil {
		t.Fatalf("acknowledge claimed message: %v", err)
	}
}

func TestRedisMoveToDeadLetter(t *testing.T) {
	if os.Getenv("REDIS_INTEGRATION") != "1" {
		t.Skip("set REDIS_INTEGRATION=1 to run Redis integration test")
	}

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	redisClient, err := NewRedisClient(addr)
	if err != nil {
		t.Fatalf("connect to Redis: %v", err)
	}
	defer redisClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	stream := "test:dlq:events:" + suffix
	deadLetterStream := "test:dlq:dead:" + suffix
	group := "test-dlq-workers-" + suffix

	defer redisClient.client.Del(context.Background(), stream, deadLetterStream)

	eventID := time.Now().UnixNano()
	eventType := "task.created"
	payload := []byte(`{"id":789,"name":"dead-letter-test"}`)

	messageID, err := redisClient.Publish(ctx, stream, eventID, eventType, payload)
	if err != nil {
		t.Fatalf("publish event: %v", err)
	}

	if err := redisClient.CreateConsumerGroup(ctx, stream, group); err != nil {
		t.Fatalf("create consumer group: %v", err)
	}

	messages, err := redisClient.ReadGroup(ctx, stream, group, "consumer-1", 10, time.Second)
	if err != nil {
		t.Fatalf("read message: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	message := messages[0]

	if message.ID != messageID {
		t.Fatalf("message ID = %q, want %q", message.ID, messageID)
	}

	attempts, err := redisClient.GetDeliveryCount(ctx, stream, group, message.ID)
	if err != nil {
		t.Fatalf("get initial delivery count: %v", err)
	}

	if attempts != 1 {
		t.Fatalf("initial delivery count = %d, want 1", attempts)
	}

	moved, err := redisClient.MoveToDeadLetter(
		ctx,
		stream,
		group,
		deadLetterStream,
		message,
		2,
		"simulated processing failure",
	)
	if err != nil {
		t.Fatalf("check dead letter threshold: %v", err)
	}

	if moved {
		t.Fatal("message moved to dead letter before reaching the attempt limit")
	}

	attempts, err = redisClient.GetDeliveryCount(ctx, stream, group, message.ID)
	if err != nil {
		t.Fatalf("get delivery count after first check: %v", err)
	}

	if attempts != 1 {
		t.Fatalf("delivery count = %d, want 1", attempts)
	}

	claimed, _, err := redisClient.ClaimPending(
		ctx,
		stream,
		group,
		"consumer-2",
		0,
		"0-0",
		10,
	)
	if err != nil {
		t.Fatalf("claim pending message: %v", err)
	}

	if len(claimed) != 1 {
		t.Fatalf("expected 1 claimed message, got %d", len(claimed))
	}

	attempts, err = redisClient.GetDeliveryCount(ctx, stream, group, message.ID)
	if err != nil {
		t.Fatalf("get delivery count after claim: %v", err)
	}

	if attempts != 2 {
		t.Fatalf("delivery count after claim = %d, want 2", attempts)
	}

	moved, err = redisClient.MoveToDeadLetter(
		ctx,
		stream,
		group,
		deadLetterStream,
		claimed[0],
		2,
		"simulated processing failure",
	)
	if err != nil {
		t.Fatalf("move message to dead letter: %v", err)
	}

	if !moved {
		t.Fatal("expected message to be moved to dead letter")
	}

	pending, err := redisClient.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  group,
		Start:  message.ID,
		End:    message.ID,
		Count:  1,
	}).Result()
	if err != nil {
		t.Fatalf("check original pending message: %v", err)
	}

	if len(pending) != 0 {
		t.Fatalf("expected original message to be acknowledged, got %d pending entries", len(pending))
	}

	deadMessages, err := redisClient.client.XRange(ctx, deadLetterStream, "-", "+").Result()
	if err != nil {
		t.Fatalf("read dead letter stream: %v", err)
	}

	if len(deadMessages) != 1 {
		t.Fatalf("expected 1 dead letter message, got %d", len(deadMessages))
	}

	values := deadMessages[0].Values

	if got := values["original_message_id"]; got != message.ID {
		t.Errorf("original_message_id = %v, want %q", got, message.ID)
	}

	if got := values["event_id"]; got != strconv.FormatInt(eventID, 10) {
		t.Errorf("event_id = %v, want %d", got, eventID)
	}

	if got := values["event_type"]; got != eventType {
		t.Errorf("event_type = %v, want %q", got, eventType)
	}

	if got := values["payload"]; got != string(payload) {
		t.Errorf("payload = %v, want %s", got, payload)
	}

	if got := values["attempts"]; got != "2" {
		t.Errorf("attempts = %v, want 2", got)
	}

	if got := values["reason"]; got != "simulated processing failure" {
		t.Errorf("reason = %v, want simulated processing failure", got)
	}
}
