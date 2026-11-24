package outbox

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

type mockEventPublisher struct {
	entryID string
	err     error
	called  bool
	eventID int64
	stream  string
	payload []byte
}

func (m *mockEventPublisher) Publish(
	ctx context.Context,
	stream string,
	eventID int64,
	eventType string,
	payload []byte,
) (string, error) {
	m.called = true
	m.stream = stream
	m.eventID = eventID
	m.payload = payload

	return m.entryID, m.err
}

func TestPublisherPublishNextSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()

	redis := &mockEventPublisher{
		entryID: "1234567890-0",
	}

	publisher := NewPublisher(db, redis, "tasks:events")

	mock.ExpectBegin()

	mock.ExpectQuery("SELECT id, event_type, payload").
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "event_type", "payload"}).
				AddRow(int64(42), "task.created", []byte(`{"id":42}`)),
		)

	mock.ExpectExec(regexp.QuoteMeta(`
		UPDATE outbox_events
		SET published_at = NOW(),
		    last_error = NULL
		WHERE id = $1
	`)).
		WithArgs(int64(42)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectCommit()

	published, err := publisher.PublishNext(context.Background())
	if err != nil {
		t.Fatalf("PublishNext returned error: %v", err)
	}

	if !published {
		t.Fatal("expected published to be true")
	}

	if !redis.called {
		t.Fatal("expected Redis Publish to be called")
	}

	if redis.eventID != 42 {
		t.Errorf("event ID = %d, want 42", redis.eventID)
	}

	if redis.stream != "tasks:events" {
		t.Errorf("stream = %q, want tasks:events", redis.stream)
	}

	if string(redis.payload) != `{"id":42}` {
		t.Errorf("payload = %s, want %s", redis.payload, `{"id":42}`)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet SQL expectations: %v", err)
	}
}

func TestPublisherPublishNextRedisError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()

	redis := &mockEventPublisher{
		err: errors.New("Redis unavailable"),
	}

	publisher := NewPublisher(db, redis, "tasks:events")

	mock.ExpectBegin()

	mock.ExpectQuery("SELECT id, event_type, payload").
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "event_type", "payload"}).
				AddRow(int64(42), "task.created", []byte(`{"id":42}`)),
		)

	mock.ExpectExec("UPDATE outbox_events").
		WithArgs(int64(42), "Redis unavailable").
		WillReturnResult(sqlmock.NewResult(0, 1))

	mock.ExpectCommit()

	published, err := publisher.PublishNext(context.Background())
	if err == nil {
		t.Fatal("expected Redis publish error")
	}

	if !published {
		t.Fatal("expected published to be true because an event was processed")
	}

	if !redis.called {
		t.Fatal("expected Redis Publish to be called")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet SQL expectations: %v", err)
	}
}

func TestPublisherPublishNextNoEvents(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sqlmock: %v", err)
	}
	defer db.Close()

	redis := &mockEventPublisher{}
	publisher := NewPublisher(db, redis, "tasks:events")

	mock.ExpectBegin()

	mock.ExpectQuery("SELECT id, event_type, payload").
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "event_type", "payload"}),
		)

	mock.ExpectRollback()

	published, err := publisher.PublishNext(context.Background())
	if err != nil {
		t.Fatalf("PublishNext returned error: %v", err)
	}

	if published {
		t.Fatal("expected published to be false")
	}

	if redis.called {
		t.Fatal("Redis Publish should not be called when there are no events")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet SQL expectations: %v", err)
	}
}
