package worker

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Arman9128/go-backend-engineering/internal/config"
	"github.com/Arman9128/go-backend-engineering/internal/database"
	"github.com/Arman9128/go-backend-engineering/internal/httpapi"
	"github.com/Arman9128/go-backend-engineering/internal/idempotency"
	"github.com/Arman9128/go-backend-engineering/internal/outbox"
	"github.com/Arman9128/go-backend-engineering/internal/queue"
	"github.com/Arman9128/go-backend-engineering/internal/storage"
	"github.com/Arman9128/go-backend-engineering/internal/task"
	"github.com/redis/go-redis/v9"
)

func TestTaskProcessingEndToEndIntegration(t *testing.T) {
	if os.Getenv("POSTGRES_INTEGRATION") != "1" ||
		os.Getenv("REDIS_INTEGRATION") != "1" {
		t.Skip("set POSTGRES_INTEGRATION=1 and REDIS_INTEGRATION=1 to run")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	schema := "worker_e2e_" + suffix
	stream := "test:worker:e2e:" + suffix
	group := "worker-e2e-" + suffix
	consumer := "worker-e2e-integration"

	if _, err := adminDB.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create test schema: %v", err)
	}
	defer func() {
		if _, err := adminDB.ExecContext(
			context.Background(),
			"DROP SCHEMA "+schema+" CASCADE",
		); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	}()

	testDB, err := database.NewPostgresDB(cfg)
	if err != nil {
		t.Fatalf("connect to test schema: %v", err)
	}
	testDB.SetMaxOpenConns(1)
	defer testDB.Close()

	if _, err := testDB.ExecContext(ctx, "SET search_path TO "+schema); err != nil {
		t.Fatalf("set test schema search_path: %v", err)
	}

	migrations, err := filepath.Glob("../../migrations/*.up.sql")
	if err != nil {
		t.Fatalf("find migrations: %v", err)
	}
	if len(migrations) != 3 {
		t.Fatalf("migration files = %d, want 3", len(migrations))
	}

	for _, migration := range migrations {
		content, err := os.ReadFile(migration)
		if err != nil {
			t.Fatalf("read migration %s: %v", migration, err)
		}
		if _, err := testDB.ExecContext(ctx, string(content)); err != nil {
			t.Fatalf("apply migration %s: %v", migration, err)
		}
	}

	redisClient, err := queue.NewRedisClient(cfg.RedisAddr)
	if err != nil {
		t.Fatalf("connect to Redis: %v", err)
	}
	defer redisClient.Close()

	rawRedis := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rawRedis.Close()
	defer func() {
		if err := rawRedis.Del(
			context.Background(),
			stream,
			stream+":dead",
		).Err(); err != nil {
			t.Errorf("delete test Redis streams: %v", err)
		}
	}()

	taskStorage := storage.NewPostgresStorage(testDB)
	taskService := task.NewService("End-to-End Test", taskStorage)

	handler := httpapi.NewHandler(taskService, testDB)
	server := httptest.NewServer(handler.Routes())
	defer server.Close()

	publisher := outbox.NewPublisher(testDB, redisClient, stream)
	processor := NewTaskProcessor(taskService)
	worker := NewWorker(redisClient, processor, stream, group, consumer)
	worker.SetIdempotencyStore(idempotency.NewPostgresStore(testDB, 2*time.Minute))

	if err := redisClient.CreateConsumerGroup(ctx, stream, group); err != nil {
		t.Fatalf("create consumer group: %v", err)
	}

	requestBody := []byte(`{
		"name": "end-to-end integration task",
		"tags": ["integration", "worker", "postgres"]
	}`)

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		server.URL+"/tasks",
		bytes.NewReader(requestBody),
	)
	if err != nil {
		t.Fatalf("create HTTP request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("POST /tasks: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusCreated {
		t.Fatalf("POST /tasks status = %d, want %d", response.StatusCode, http.StatusCreated)
	}

	var created struct {
		ID     int64    `json:"id"`
		Name   string   `json:"name"`
		Status string   `json:"status"`
		Tags   []string `json:"tags"`
	}
	if err := json.NewDecoder(response.Body).Decode(&created); err != nil {
		t.Fatalf("decode create task response: %v", err)
	}

	if created.ID <= 0 {
		t.Fatalf("created task ID = %d, want positive ID", created.ID)
	}
	if created.Status != task.StatusPending {
		t.Fatalf("initial task status = %q, want pending", created.Status)
	}

	var eventID int64
	var publishedAt sql.NullTime

	err = testDB.QueryRowContext(ctx, `
		SELECT id, published_at
		FROM outbox_events
		WHERE aggregate_id = $1
		  AND event_type = 'task.created'
	`, created.ID).Scan(&eventID, &publishedAt)
	if err != nil {
		t.Fatalf("read task outbox event: %v", err)
	}
	if eventID <= 0 {
		t.Fatalf("outbox event ID = %d, want positive ID", eventID)
	}
	if publishedAt.Valid {
		t.Fatal("outbox event was published before Publisher.PublishNext")
	}

	processed, err := publisher.PublishNext(ctx)
	if err != nil {
		t.Fatalf("publish outbox event: %v", err)
	}
	if !processed {
		t.Fatal("Publisher.PublishNext did not publish an event")
	}

	err = testDB.QueryRowContext(ctx, `
		SELECT published_at
		FROM outbox_events
		WHERE id = $1
	`, eventID).Scan(&publishedAt)
	if err != nil {
		t.Fatalf("check published outbox event: %v", err)
	}
	if !publishedAt.Valid {
		t.Fatal("outbox event published_at was not set")
	}

	messages, err := redisClient.ReadGroup(
		ctx,
		stream,
		group,
		consumer,
		10,
		time.Second,
	)
	if err != nil {
		t.Fatalf("read published Redis event: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("Redis messages = %d, want 1", len(messages))
	}
	if messages[0].EventID != eventID {
		t.Fatalf("Redis event ID = %d, want %d", messages[0].EventID, eventID)
	}

	worker.processMessages(ctx, messages)

	var taskStatus string
	if err := testDB.QueryRowContext(ctx,
		"SELECT status FROM tasks WHERE id = $1",
		created.ID,
	).Scan(&taskStatus); err != nil {
		t.Fatalf("read processed task status: %v", err)
	}
	if taskStatus != task.StatusCompleted {
		t.Fatalf("task status after processing = %q, want completed", taskStatus)
	}

	var eventStatus string
	if err := testDB.QueryRowContext(ctx, `
		SELECT status
		FROM processed_events
		WHERE event_id = $1
	`, eventID).Scan(&eventStatus); err != nil {
		t.Fatalf("read processed event status: %v", err)
	}
	if eventStatus != "completed" {
		t.Fatalf("processed event status = %q, want completed", eventStatus)
	}

	getRequest, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		fmt.Sprintf("%s/tasks/%d", server.URL, created.ID),
		nil,
	)
	if err != nil {
		t.Fatalf("create GET task request: %v", err)
	}

	getResponse, err := http.DefaultClient.Do(getRequest)
	if err != nil {
		t.Fatalf("GET /tasks/{id}: %v", err)
	}
	defer getResponse.Body.Close()

	if getResponse.StatusCode != http.StatusOK {
		t.Fatalf("GET /tasks/{id} status = %d, want %d",
			getResponse.StatusCode, http.StatusOK)
	}

	var fetched struct {
		ID     int64    `json:"id"`
		Name   string   `json:"name"`
		Status string   `json:"status"`
		Tags   []string `json:"tags"`
	}
	if err := json.NewDecoder(getResponse.Body).Decode(&fetched); err != nil {
		t.Fatalf("decode GET task response: %v", err)
	}
	if fetched.Status != task.StatusCompleted {
		t.Fatalf("GET task status = %q, want completed", fetched.Status)
	}
	if fetched.ID != created.ID || fetched.Name != created.Name {
		t.Fatalf("GET task identity does not match created task")
	}

	if _, err := redisClient.Publish(
		ctx,
		stream,
		eventID,
		"task.created",
		messages[0].Payload,
	); err != nil {
		t.Fatalf("publish duplicate event: %v", err)
	}

	duplicates, err := redisClient.ReadGroup(
		ctx,
		stream,
		group,
		consumer,
		10,
		time.Second,
	)
	if err != nil {
		t.Fatalf("read duplicate event: %v", err)
	}
	if len(duplicates) != 1 {
		t.Fatalf("duplicate messages = %d, want 1", len(duplicates))
	}

	worker.processMessages(ctx, duplicates)

	if err := testDB.QueryRowContext(ctx,
		"SELECT status FROM tasks WHERE id = $1",
		created.ID,
	).Scan(&taskStatus); err != nil {
		t.Fatalf("read task status after duplicate: %v", err)
	}
	if taskStatus != task.StatusCompleted {
		t.Fatalf("task status after duplicate = %q, want completed", taskStatus)
	}

	var processingAttempts int
	if err := testDB.QueryRowContext(ctx, `
		SELECT attempts
		FROM processed_events
		WHERE event_id = $1
	`, eventID).Scan(&processingAttempts); err != nil {
		t.Fatalf("read idempotency attempts after duplicate: %v", err)
	}
	if processingAttempts != 1 {
		t.Fatalf("idempotency attempts = %d, want 1", processingAttempts)
	}

	deadLetterMessages, err := rawRedis.XRange(
		ctx,
		stream+":dead",
		"-",
		"+",
	).Result()
	if err != nil {
		t.Fatalf("read dead letter stream: %v", err)
	}
	if len(deadLetterMessages) != 0 {
		t.Fatalf("dead letter messages = %d, want 0", len(deadLetterMessages))
	}

	pending, err := rawRedis.XPendingExt(ctx, &redis.XPendingExtArgs{
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

	t.Logf(
		"End-to-end task processing succeeded: task_id=%d event_id=%d",
		created.ID,
		eventID,
	)
}
