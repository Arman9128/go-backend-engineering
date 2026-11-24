package queue

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"
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
