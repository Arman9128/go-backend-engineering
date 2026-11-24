package outbox

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Arman9128/go-backend-engineering/internal/config"
	"github.com/Arman9128/go-backend-engineering/internal/database"
	"github.com/Arman9128/go-backend-engineering/internal/queue"
	"github.com/redis/go-redis/v9"
)

func TestPublisherPublishNextIntegration(t *testing.T) {
	if os.Getenv("POSTGRES_INTEGRATION") != "1" ||
		os.Getenv("REDIS_INTEGRATION") != "1" {
		t.Skip("set POSTGRES_INTEGRATION=1 and REDIS_INTEGRATION=1 to run")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	adminDB, err := database.NewPostgresDB(cfg)
	if err != nil {
		t.Fatalf("connect to PostgreSQL: %v", err)
	}
	defer adminDB.Close()

	schema := fmt.Sprintf("outbox_test_%d", time.Now().UnixNano())
	stream := schema + ":events"

	if _, err := adminDB.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create test schema: %v", err)
	}
	defer func() {
		_, err := adminDB.ExecContext(
			context.Background(),
			"DROP SCHEMA "+schema+" CASCADE",
		)
		if err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	}()

	_, err = adminDB.ExecContext(ctx, `
		CREATE TABLE `+schema+`.outbox_events (
			id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			aggregate_type VARCHAR(50) NOT NULL,
			aggregate_id BIGINT NOT NULL,
			event_type VARCHAR(100) NOT NULL,
			payload JSONB NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			published_at TIMESTAMPTZ,
			attempts INTEGER NOT NULL DEFAULT 0,
			last_error TEXT,
			CONSTRAINT outbox_events_attempts_check CHECK (attempts >= 0)
		)
	`)
	if err != nil {
		t.Fatalf("create test outbox table: %v", err)
	}

	testDB, err := database.NewPostgresDB(cfg)
	if err != nil {
		t.Fatalf("connect to test schema: %v", err)
	}
	testDB.SetMaxOpenConns(1)
	defer testDB.Close()

	if _, err := testDB.ExecContext(ctx, "SET search_path TO "+schema); err != nil {
		t.Fatalf("set test schema search_path: %v", err)
	}

	payload := []byte(`{"id":123,"name":"outbox-integration-test"}`)

	var eventID int64
	err = testDB.QueryRowContext(ctx, `
		INSERT INTO outbox_events (
			aggregate_type,
			aggregate_id,
			event_type,
			payload
		)
		VALUES ($1, $2, $3, $4)
		RETURNING id
	`, "task", 123, "task.created", payload).Scan(&eventID)
	if err != nil {
		t.Fatalf("insert test outbox event: %v", err)
	}

	redisPublisher, err := queue.NewRedisClient(cfg.RedisAddr)
	if err != nil {
		t.Fatalf("connect to Redis: %v", err)
	}
	defer redisPublisher.Close()

	redisReader := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})
	defer redisReader.Close()
	defer redisReader.Del(context.Background(), stream)

	publisher := NewPublisher(testDB, redisPublisher, stream)

	processed, err := publisher.PublishNext(ctx)
	if err != nil {
		t.Fatalf("PublishNext: %v", err)
	}
	if !processed {
		t.Fatal("expected an event to be processed")
	}

	var publishedAt sql.NullTime
	var attempts int
	var lastError sql.NullString

	err = testDB.QueryRowContext(ctx, `
		SELECT published_at, attempts, last_error
		FROM outbox_events
		WHERE id = $1
	`, eventID).Scan(&publishedAt, &attempts, &lastError)
	if err != nil {
		t.Fatalf("read published event: %v", err)
	}

	if !publishedAt.Valid {
		t.Fatal("expected published_at to be set")
	}
	if attempts != 0 {
		t.Errorf("attempts = %d, want 0", attempts)
	}
	if lastError.Valid {
		t.Errorf("last_error = %q, want NULL", lastError.String)
	}

	entries, err := redisReader.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("read Redis Stream: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("Redis Stream entries = %d, want 1", len(entries))
	}

	fields := entries[0].Values

	if got := fmt.Sprint(fields["event_id"]); got != fmt.Sprint(eventID) {
		t.Errorf("Redis event_id = %q, want %d", got, eventID)
	}
	if got := fmt.Sprint(fields["event_type"]); got != "task.created" {
		t.Errorf("Redis event_type = %q, want task.created", got)
	}
	var gotPayload bytes.Buffer
	if err := json.Compact(&gotPayload, []byte(fmt.Sprint(fields["payload"]))); err != nil {
		t.Fatalf("compact Redis payload: %v", err)
	}

	var wantPayload bytes.Buffer
	if err := json.Compact(&wantPayload, payload); err != nil {
		t.Fatalf("compact expected payload: %v", err)
	}

	if !bytes.Equal(gotPayload.Bytes(), wantPayload.Bytes()) {
		t.Errorf("Redis payload = %s, want %s", gotPayload.String(), wantPayload.String())
	}

	t.Logf("Successfully published outbox event %d to Redis Stream %s", eventID, stream)
}
