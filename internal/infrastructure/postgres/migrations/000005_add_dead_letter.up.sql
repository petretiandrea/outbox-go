-- "Quarantine" is renamed to "dead letter". A row is dead-lettered once it has
-- failed permanently (or exhausted max_attempts) AND is parked or acknowledged
-- by the dead-letter queue (DLQ).
--
-- failed_at          the row failed permanently; it waits for DLQ delivery (or
--                    is parked when no DLQ is configured).
-- dead_lettered_at   the row left the claim queue for good: it was acknowledged
--                    by the DLQ, or parked because no DLQ is configured.
-- dead_letter_attempts  failed DLQ publish attempts, used for DLQ retry backoff.
ALTER TABLE outbox_messages RENAME COLUMN quarantined_at TO dead_lettered_at;

ALTER TABLE outbox_messages
    ADD COLUMN IF NOT EXISTS failed_at TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS dead_letter_attempts INTEGER NOT NULL DEFAULT 0;

-- Rows quarantined by migration 000004 are failed and parked.
UPDATE outbox_messages SET failed_at = dead_lettered_at WHERE dead_lettered_at IS NOT NULL AND failed_at IS NULL;

DROP INDEX IF EXISTS idx_outbox_messages_claimable;
CREATE INDEX IF NOT EXISTS idx_outbox_messages_claimable
    ON outbox_messages (occurred_at, id)
    WHERE dead_lettered_at IS NULL;
