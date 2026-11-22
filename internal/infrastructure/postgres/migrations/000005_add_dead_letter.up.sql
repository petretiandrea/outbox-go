-- "Quarantine" is renamed to "dead letter". The outbox table is the
-- dead-letter queue: a row that failed permanently (or exhausted max_attempts)
-- keeps its last_error and attempts, gets dead_lettered_at and is no longer
-- claimed.
ALTER TABLE outbox_messages RENAME COLUMN quarantined_at TO dead_lettered_at;

DROP INDEX IF EXISTS idx_outbox_messages_claimable;
CREATE INDEX IF NOT EXISTS idx_outbox_messages_claimable
    ON outbox_messages (occurred_at, id)
    WHERE dead_lettered_at IS NULL;

-- Supports counting and listing the dead-lettered rows.
CREATE INDEX IF NOT EXISTS idx_outbox_messages_dead_lettered
    ON outbox_messages (dead_lettered_at)
    WHERE dead_lettered_at IS NOT NULL;
