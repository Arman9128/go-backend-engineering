CREATE TABLE outbox_events (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    aggregate_type VARCHAR(50) NOT NULL,
    aggregate_id BIGINT NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,

    CONSTRAINT outbox_events_attempts_check
        CHECK (attempts >= 0)
);

CREATE INDEX idx_outbox_events_unpublished
    ON outbox_events (created_at, id)
    WHERE published_at IS NULL;
