-- next_attempt_at delays the retry of a failed row (backoff) without blocking
-- other rows; quarantined_at removes a poison row from the claim queue while
-- keeping it in the table for manual inspection and recovery.
ALTER TABLE outbox_messages
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS quarantined_at TIMESTAMPTZ NULL;

-- Partial index over the claimable queue only; quarantined rows are excluded.
CREATE INDEX IF NOT EXISTS idx_outbox_messages_claimable
    ON outbox_messages (occurred_at, id)
    WHERE quarantined_at IS NULL;
