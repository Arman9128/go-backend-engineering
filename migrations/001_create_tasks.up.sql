CREATE TABLE tasks (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tags TEXT[] NOT NULL DEFAULT '{}',

    CONSTRAINT tasks_status_check
        CHECK (status IN (
            'pending',
            'processing',
            'completed',
            'failed',
            'rejected'
        ))
);

CREATE INDEX idx_tasks_status_created_at
    ON tasks (status, created_at);
