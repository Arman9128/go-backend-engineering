package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type EventPublisher interface {
	Publish(
		ctx context.Context,
		stream string,
		eventID int64,
		eventType string,
		payload []byte,
	) (string, error)
}

type Publisher struct {
	db     *sql.DB
	redis  EventPublisher
	stream string
}

func NewPublisher(
	db *sql.DB,
	redis EventPublisher,
	stream string,
) *Publisher {
	return &Publisher{
		db:     db,
		redis:  redis,
		stream: stream,
	}
}

func (p *Publisher) PublishNext(ctx context.Context) (bool, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin outbox transaction: %w", err)
	}
	defer tx.Rollback()

	var (
		eventID   int64
		eventType string
		payload   []byte
	)

	err = tx.QueryRowContext(ctx, `
		SELECT id, event_type, payload
		FROM outbox_events
		WHERE published_at IS NULL
		ORDER BY created_at, id
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`).Scan(&eventID, &eventType, &payload)

	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("select outbox event: %w", err)
	}

	_, publishErr := p.redis.Publish(
		ctx,
		p.stream,
		eventID,
		eventType,
		payload,
	)

	if publishErr != nil {
		_, err = tx.ExecContext(ctx, `
			UPDATE outbox_events
			SET attempts = attempts + 1,
			    last_error = $2
			WHERE id = $1
		`, eventID, publishErr.Error())

		if err != nil {
			return false, fmt.Errorf(
				"record outbox publish failure: %w",
				err,
			)
		}

		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf(
				"commit outbox publish failure: %w",
				err,
			)
		}

		return true, fmt.Errorf(
			"publish outbox event %d: %w",
			eventID,
			publishErr,
		)
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE outbox_events
		SET published_at = NOW(),
		    last_error = NULL
		WHERE id = $1
	`, eventID)

	if err != nil {
		return false, fmt.Errorf("mark outbox event as published: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit published outbox event: %w", err)
	}

	return true, nil
}
