package idempotency

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

var ErrLeaseLost = errors.New("idempotency lease lost")

type Store interface {
	Acquire(ctx context.Context, eventID int64) (token string, acquired bool, completed bool, err error)
	Renew(ctx context.Context, eventID int64, token string) error
	Complete(ctx context.Context, eventID int64, token string) error
	Release(ctx context.Context, eventID int64, token string, processingErr error) error
}

type PostgresStore struct {
	db          *sql.DB
	leasePeriod time.Duration
}

func NewPostgresStore(db *sql.DB, leasePeriod time.Duration) *PostgresStore {
	return &PostgresStore{
		db:          db,
		leasePeriod: leasePeriod,
	}
}

func (s *PostgresStore) Acquire(
	ctx context.Context,
	eventID int64,
) (string, bool, bool, error) {
	if s.db == nil {
		return "", false, false, errors.New("database connection is nil")
	}

	if eventID <= 0 {
		return "", false, false, fmt.Errorf("invalid event ID %d", eventID)
	}

	if s.leasePeriod <= 0 {
		return "", false, false, errors.New("lease period must be positive")
	}

	token, err := generateToken()
	if err != nil {
		return "", false, false, fmt.Errorf("generate lock token: %w", err)
	}

	var acquiredToken string

	err = s.db.QueryRowContext(ctx, `
		INSERT INTO processed_events (
			event_id,
			status,
			lock_token,
			locked_until
		)
		VALUES ($1, 'processing', $2, NOW() + $3::interval)

		ON CONFLICT (event_id) DO UPDATE
		SET
			lock_token = EXCLUDED.lock_token,
			locked_until = EXCLUDED.locked_until,
			attempts = processed_events.attempts + 1,
			last_error = NULL,
			updated_at = NOW()
		WHERE
			processed_events.status = 'processing'
			AND processed_events.locked_until <= NOW()

		RETURNING lock_token::text
	`, eventID, token, s.leasePeriod.String()).Scan(&acquiredToken)

	if err == nil {
		return acquiredToken, true, false, nil
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return "", false, false, fmt.Errorf("acquire event %d: %w", eventID, err)
	}

	var status string

	err = s.db.QueryRowContext(
		ctx,
		"SELECT status FROM processed_events WHERE event_id = $1",
		eventID,
	).Scan(&status)

	if err != nil {
		return "", false, false, fmt.Errorf("check event %d status: %w", eventID, err)
	}

	if status == "completed" {
		return "", false, true, nil
	}

	return "", false, false, nil
}

func (s *PostgresStore) Renew(
	ctx context.Context,
	eventID int64,
	token string,
) error {
	if s.db == nil {
		return errors.New("database connection is nil")
	}

	if eventID <= 0 || token == "" {
		return errors.New("event ID and lock token are required")
	}

	if s.leasePeriod <= 0 {
		return errors.New("lease period must be positive")
	}

	result, err := s.db.ExecContext(ctx, `
		UPDATE processed_events
		SET
			locked_until = NOW() + $3::interval,
			updated_at = NOW()
		WHERE
			event_id = $1
			AND status = 'processing'
			AND lock_token = $2::uuid
			AND locked_until > NOW()
	`, eventID, token, s.leasePeriod.String())
	if err != nil {
		return fmt.Errorf("renew event %d lease: %w", eventID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get affected rows for event %d: %w", eventID, err)
	}

	if rowsAffected != 1 {
		return fmt.Errorf("%w: cannot renew event %d", ErrLeaseLost, eventID)
	}

	return nil
}

func (s *PostgresStore) Complete(
	ctx context.Context,
	eventID int64,
	token string,
) error {
	if s.db == nil {
		return errors.New("database connection is nil")
	}

	if eventID <= 0 || token == "" {
		return errors.New("event ID and lock token are required")
	}

	result, err := s.db.ExecContext(ctx, `
		UPDATE processed_events
		SET
			status = 'completed',
			lock_token = NULL,
			locked_until = NULL,
			processed_at = NOW(),
			last_error = NULL,
			updated_at = NOW()
		WHERE
			event_id = $1
			AND status = 'processing'
			AND lock_token = $2::uuid
			AND locked_until > NOW()
	`, eventID, token)

	if err != nil {
		return fmt.Errorf("complete event %d: %w", eventID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get affected rows for event %d: %w", eventID, err)
	}

	if rowsAffected != 1 {
		return fmt.Errorf("%w: cannot complete event %d", ErrLeaseLost, eventID)
	}

	return nil
}

func (s *PostgresStore) Release(
	ctx context.Context,
	eventID int64,
	token string,
	processingErr error,
) error {
	if s.db == nil {
		return errors.New("database connection is nil")
	}

	if eventID <= 0 || token == "" {
		return errors.New("event ID and lock token are required")
	}

	if processingErr == nil {
		return errors.New("processing error is required")
	}

	result, err := s.db.ExecContext(ctx, `
		UPDATE processed_events
		SET
			locked_until = NOW(),
			last_error = $3,
			updated_at = NOW()
		WHERE
			event_id = $1
			AND status = 'processing'
			AND lock_token = $2::uuid
			AND locked_until > NOW()
	`, eventID, token, processingErr.Error())

	if err != nil {
		return fmt.Errorf("release event %d: %w", eventID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get affected rows for event %d: %w", eventID, err)
	}

	if rowsAffected != 1 {
		return fmt.Errorf("%w: cannot release event %d", ErrLeaseLost, eventID)
	}

	return nil
}

func generateToken() (string, error) {
	var token [16]byte

	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}

	token[6] = (token[6] & 0x0f) | 0x40
	token[8] = (token[8] & 0x3f) | 0x80

	encoded := hex.EncodeToString(token[:])

	return fmt.Sprintf(
		"%s-%s-%s-%s-%s",
		encoded[0:8],
		encoded[8:12],
		encoded[12:16],
		encoded[16:20],
		encoded[20:32],
	), nil
}
