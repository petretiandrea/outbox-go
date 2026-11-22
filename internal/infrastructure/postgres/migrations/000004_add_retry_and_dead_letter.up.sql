-- next_attempt_at delays the retry of a failed row (backoff) without blocking
-- other rows.
ALTER TABLE outbox_messages
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NULL;

-- The dead-letter queue. A message that failed permanently, or exhausted
-- max_attempts, is moved out of outbox_messages into this table in a single
-- statement, together with its failure details, so it is never claimed again.
CREATE TABLE IF NOT EXISTS outbox_dead_letters (
    id TEXT PRIMARY KEY,
    channel TEXT NOT NULL,
    affinity_key TEXT NULL,
    payload BYTEA NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL,
    attempts INTEGER NOT NULL,
    last_error TEXT NULL,
    dead_lettered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_outbox_dead_letters_dead_lettered_at
    ON outbox_dead_letters (dead_lettered_at);
