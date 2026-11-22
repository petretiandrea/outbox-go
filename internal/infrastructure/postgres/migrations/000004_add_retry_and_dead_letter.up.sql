-- next_attempt_at delays the retry of a failed row (backoff) without blocking
-- other rows. dead_lettered_at removes a row that failed permanently, or
-- exhausted max_attempts, from the claim queue while keeping it in the table,
-- which is the dead-letter queue, with last_error and attempts for inspection
-- and replay.
ALTER TABLE outbox_messages
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS dead_lettered_at TIMESTAMPTZ NULL;

-- Partial index over the claimable queue only; dead-lettered rows are excluded.
CREATE INDEX IF NOT EXISTS idx_outbox_messages_claimable
    ON outbox_messages (occurred_at, id)
    WHERE dead_lettered_at IS NULL;

-- Supports counting and listing the dead-lettered rows.
CREATE INDEX IF NOT EXISTS idx_outbox_messages_dead_lettered
    ON outbox_messages (dead_lettered_at)
    WHERE dead_lettered_at IS NOT NULL;
