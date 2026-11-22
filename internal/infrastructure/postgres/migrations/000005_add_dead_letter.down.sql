DROP INDEX IF EXISTS idx_outbox_messages_dead_lettered;
DROP INDEX IF EXISTS idx_outbox_messages_claimable;

ALTER TABLE outbox_messages RENAME COLUMN dead_lettered_at TO quarantined_at;

CREATE INDEX IF NOT EXISTS idx_outbox_messages_claimable
    ON outbox_messages (occurred_at, id)
    WHERE quarantined_at IS NULL;
