DROP INDEX IF EXISTS idx_outbox_messages_dead_lettered;
DROP INDEX IF EXISTS idx_outbox_messages_claimable;

ALTER TABLE outbox_messages
    DROP COLUMN IF EXISTS dead_lettered_at,
    DROP COLUMN IF EXISTS next_attempt_at;
