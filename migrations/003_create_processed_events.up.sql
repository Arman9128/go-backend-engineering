CREATE TABLE processed_events (
    event_id BIGINT PRIMARY KEY,
    status VARCHAR(20) NOT NULL,
    lock_token UUID,
    locked_until TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 1,
    processed_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT processed_events_status_check
        CHECK (status IN ('processing', 'completed')),

    CONSTRAINT processed_events_attempts_check
        CHECK (attempts > 0),

    CONSTRAINT processed_events_state_check
        CHECK (
            (status = 'processing' AND lock_token IS NOT NULL AND locked_until IS NOT NULL AND processed_at IS NULL)
            OR
            (status = 'completed' AND lock_token IS NULL AND locked_until IS NULL AND processed_at IS NOT NULL)
        )
);

CREATE INDEX idx_processed_events_locked_until
    ON processed_events (locked_until)
    WHERE status = 'processing';
