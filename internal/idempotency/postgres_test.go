package idempotency

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Arman9128/go-backend-engineering/internal/config"
	"github.com/Arman9128/go-backend-engineering/internal/database"
)

func newIntegrationStore(t *testing.T, leasePeriod time.Duration) (*PostgresStore, func() int64) {
	t.Helper()

	if os.Getenv("POSTGRES_INTEGRATION") != "1" {
		t.Skip("PostgreSQL integration test is disabled")
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	db, err := database.NewPostgresDB(cfg)
	if err != nil {
		t.Fatalf("connect to PostgreSQL: %v", err)
	}

	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close PostgreSQL connection: %v", err)
		}
	})

	store := NewPostgresStore(db, leasePeriod)

	var sequence atomic.Int64
	baseID := time.Now().UnixNano()

	newEventID := func() int64 {
		eventID := baseID + sequence.Add(1)

		t.Cleanup(func() {
			_, err := db.ExecContext(
				context.Background(),
				"DELETE FROM processed_events WHERE event_id = $1",
				eventID,
			)
			if err != nil {
				t.Errorf("cleanup event %d: %v", eventID, err)
			}
		})

		return eventID
	}

	return store, newEventID
}

func TestPostgresStoreAcquireAndComplete(t *testing.T) {
	store, newEventID := newIntegrationStore(t, 30*time.Second)
	ctx := context.Background()
	eventID := newEventID()

	token, acquired, completed, err := store.Acquire(ctx, eventID)
	if err != nil {
		t.Fatalf("acquire event: %v", err)
	}

	if !acquired || completed || token == "" {
		t.Fatalf("unexpected acquire result: token=%q acquired=%v completed=%v",
			token, acquired, completed)
	}

	secondToken, acquired, completed, err := store.Acquire(ctx, eventID)
	if err != nil {
		t.Fatalf("acquire active event again: %v", err)
	}

	if acquired || completed || secondToken != "" {
		t.Fatalf("expected active event to be unavailable: token=%q acquired=%v completed=%v",
			secondToken, acquired, completed)
	}

	if err := store.Complete(ctx, eventID, token); err != nil {
		t.Fatalf("complete event: %v", err)
	}

	_, acquired, completed, err = store.Acquire(ctx, eventID)
	if err != nil {
		t.Fatalf("acquire completed event: %v", err)
	}

	if acquired || !completed {
		t.Fatalf("expected completed event: acquired=%v completed=%v",
			acquired, completed)
	}

	var (
		status    string
		attempts  int
		processed time.Time
	)

	err = store.db.QueryRowContext(ctx, `
		SELECT status, attempts, processed_at
		FROM processed_events
		WHERE event_id = $1
	`, eventID).Scan(&status, &attempts, &processed)
	if err != nil {
		t.Fatalf("query processed event: %v", err)
	}

	if status != "completed" {
		t.Errorf("expected status completed, got %q", status)
	}

	if attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", attempts)
	}

	if processed.IsZero() {
		t.Error("expected processed_at to be set")
	}
}

func TestPostgresStoreReleaseAllowsRetry(t *testing.T) {
	store, newEventID := newIntegrationStore(t, 30*time.Second)
	ctx := context.Background()
	eventID := newEventID()

	token, acquired, _, err := store.Acquire(ctx, eventID)
	if err != nil {
		t.Fatalf("acquire event: %v", err)
	}

	if !acquired {
		t.Fatal("expected initial acquisition")
	}

	processingErr := errors.New("temporary processing failure")

	if err := store.Release(ctx, eventID, token, processingErr); err != nil {
		t.Fatalf("release event: %v", err)
	}

	retryToken, acquired, completed, err := store.Acquire(ctx, eventID)
	if err != nil {
		t.Fatalf("retry acquire: %v", err)
	}

	if !acquired || completed || retryToken == "" {
		t.Fatalf("expected retry acquisition: token=%q acquired=%v completed=%v",
			retryToken, acquired, completed)
	}

	if retryToken == token {
		t.Fatal("expected a new lock token after retry")
	}

	var (
		attempts int

		lastError sql.NullString
	)

	err = store.db.QueryRowContext(ctx, `
		SELECT attempts, last_error
		FROM processed_events
		WHERE event_id = $1
	`, eventID).Scan(&attempts, &lastError)
	if err != nil {
		t.Fatalf("query retry state: %v", err)
	}

	if attempts != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts)
	}

	if lastError.Valid {

		t.Errorf("expected last_error to be NULL, got %q", lastError.String)

	}
}

func TestPostgresStoreExpiredLeaseCanBeReacquired(t *testing.T) {
	store, newEventID := newIntegrationStore(t, 100*time.Millisecond)
	ctx := context.Background()
	eventID := newEventID()

	oldToken, acquired, _, err := store.Acquire(ctx, eventID)
	if err != nil {
		t.Fatalf("initial acquire: %v", err)
	}

	if !acquired {
		t.Fatal("expected initial acquisition")
	}

	time.Sleep(200 * time.Millisecond)

	newToken, acquired, completed, err := store.Acquire(ctx, eventID)
	if err != nil {
		t.Fatalf("reacquire expired event: %v", err)
	}

	if !acquired || completed || newToken == "" {
		t.Fatalf("expected expired lease to be reacquired: token=%q acquired=%v completed=%v",
			newToken, acquired, completed)
	}

	if newToken == oldToken {
		t.Fatal("expected a new token after lease expiration")
	}

	err = store.Complete(ctx, eventID, oldToken)
	if !errors.Is(err, ErrLeaseLost) {
		t.Errorf("expected ErrLeaseLost for old token, got %v", err)
	}

	if err := store.Complete(ctx, eventID, newToken); err != nil {
		t.Fatalf("complete with new token: %v", err)
	}
}

func TestPostgresStoreConcurrentAcquire(t *testing.T) {
	store, newEventID := newIntegrationStore(t, 30*time.Second)
	ctx := context.Background()
	eventID := newEventID()

	const workers = 12

	var (
		wg          sync.WaitGroup
		acquired    atomic.Int32
		errorsCount atomic.Int32
	)

	start := make(chan struct{})

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			<-start

			_, ok, completed, err := store.Acquire(ctx, eventID)
			if err != nil {
				errorsCount.Add(1)
				t.Errorf("concurrent acquire: %v", err)
				return
			}

			if ok && !completed {
				acquired.Add(1)
			}
		}()
	}

	close(start)
	wg.Wait()

	if errorsCount.Load() != 0 {
		t.Fatalf("expected no errors, got %d", errorsCount.Load())
	}

	if got := acquired.Load(); got != 1 {
		t.Fatalf("expected exactly one successful acquisition, got %d", got)
	}

	var (
		status   string
		attempts int
	)

	err := store.db.QueryRowContext(ctx, `
		SELECT status, attempts
		FROM processed_events
		WHERE event_id = $1
	`, eventID).Scan(&status, &attempts)
	if err != nil {
		t.Fatalf("query concurrent event: %v", err)
	}

	if status != "processing" {
		t.Errorf("expected processing status, got %q", status)
	}

	if attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", attempts)
	}
}

func TestPostgresStoreRejectsInvalidInput(t *testing.T) {
	store, _ := newIntegrationStore(t, 30*time.Second)
	ctx := context.Background()

	tests := []struct {
		name    string
		eventID int64
	}{
		{name: "zero event ID", eventID: 0},
		{name: "negative event ID", eventID: -1},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, acquired, completed, err := store.Acquire(ctx, test.eventID)

			if err == nil {
				t.Fatal("expected validation error")
			}

			if acquired || completed {
				t.Fatalf("unexpected result: acquired=%v completed=%v",
					acquired, completed)
			}
		})
	}

	if err := store.Complete(ctx, 1, ""); err == nil {
		t.Error("expected Complete to reject empty token")
	}

	if err := store.Release(ctx, 1, "token", nil); err == nil {
		t.Error("expected Release to reject nil processing error")
	}

	invalidLeaseStore := NewPostgresStore(store.db, 0)
	if err := invalidLeaseStore.Renew(ctx, 1, "token"); err == nil {
		t.Error("expected Renew to reject non-positive lease period")
	}
}

func TestPostgresStoreRenewExtendsLease(t *testing.T) {
	store, newEventID := newIntegrationStore(t, 800*time.Millisecond)
	ctx := context.Background()
	eventID := newEventID()

	token, acquired, _, err := store.Acquire(ctx, eventID)
	if err != nil {
		t.Fatalf("acquire event: %v", err)
	}
	if !acquired {
		t.Fatal("expected initial acquisition")
	}

	time.Sleep(500 * time.Millisecond)

	if err := store.Renew(ctx, eventID, token); err != nil {
		t.Fatalf("renew lease: %v", err)
	}

	// The original lease would have expired by now.
	time.Sleep(500 * time.Millisecond)

	_, acquired, completed, err := store.Acquire(ctx, eventID)
	if err != nil {
		t.Fatalf("acquire event after renewal: %v", err)
	}
	if acquired || completed {
		t.Fatalf("expected renewed lease to remain active: acquired=%v completed=%v",
			acquired, completed)
	}

	if err := store.Complete(ctx, eventID, token); err != nil {
		t.Fatalf("complete event after renewal: %v", err)
	}
}

func TestPostgresStoreRenewRejectsExpiredLease(t *testing.T) {
	store, newEventID := newIntegrationStore(t, 200*time.Millisecond)
	ctx := context.Background()
	eventID := newEventID()

	oldToken, acquired, _, err := store.Acquire(ctx, eventID)
	if err != nil {
		t.Fatalf("acquire event: %v", err)
	}
	if !acquired {
		t.Fatal("expected initial acquisition")
	}

	time.Sleep(400 * time.Millisecond)

	err = store.Renew(ctx, eventID, oldToken)
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expected ErrLeaseLost when renewing expired lease, got %v", err)
	}

	newToken, acquired, completed, err := store.Acquire(ctx, eventID)
	if err != nil {
		t.Fatalf("reacquire expired event: %v", err)
	}
	if !acquired || completed || newToken == "" {
		t.Fatalf("expected expired lease to be reacquired: token=%q acquired=%v completed=%v",
			newToken, acquired, completed)
	}

	if newToken == oldToken {
		t.Fatal("expected a new token after lease expiration")
	}

	if err := store.Complete(ctx, eventID, newToken); err != nil {
		t.Fatalf("complete event with new token: %v", err)
	}
}

func TestPostgresStoreCompleteRejectsExpiredLease(t *testing.T) {
	store, newEventID := newIntegrationStore(t, 200*time.Millisecond)
	ctx := context.Background()
	eventID := newEventID()

	oldToken, acquired, _, err := store.Acquire(ctx, eventID)
	if err != nil {
		t.Fatalf("acquire event: %v", err)
	}
	if !acquired {
		t.Fatal("expected initial acquisition")
	}

	time.Sleep(400 * time.Millisecond)

	err = store.Complete(ctx, eventID, oldToken)
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expected ErrLeaseLost when completing expired lease, got %v", err)
	}

	newToken, acquired, completed, err := store.Acquire(ctx, eventID)
	if err != nil {
		t.Fatalf("reacquire expired event: %v", err)
	}
	if !acquired || completed || newToken == "" {
		t.Fatalf("expected expired lease to be reacquired: token=%q acquired=%v completed=%v",
			newToken, acquired, completed)
	}

	if err := store.Complete(ctx, eventID, newToken); err != nil {
		t.Fatalf("complete event with new token: %v", err)
	}
}

func TestPostgresStoreReleaseRejectsExpiredLease(t *testing.T) {
	store, newEventID := newIntegrationStore(t, 200*time.Millisecond)
	ctx := context.Background()
	eventID := newEventID()

	oldToken, acquired, _, err := store.Acquire(ctx, eventID)
	if err != nil {
		t.Fatalf("acquire event: %v", err)
	}
	if !acquired {
		t.Fatal("expected initial acquisition")
	}

	time.Sleep(400 * time.Millisecond)

	err = store.Release(ctx, eventID, oldToken, errors.New("processing failed"))
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expected ErrLeaseLost when releasing expired lease, got %v", err)
	}

	newToken, acquired, completed, err := store.Acquire(ctx, eventID)
	if err != nil {
		t.Fatalf("reacquire expired event: %v", err)
	}
	if !acquired || completed || newToken == "" {
		t.Fatalf("expected expired lease to be reacquired: token=%q acquired=%v completed=%v",
			newToken, acquired, completed)
	}

	if err := store.Complete(ctx, eventID, newToken); err != nil {
		t.Fatalf("complete event with new token: %v", err)
	}
}
