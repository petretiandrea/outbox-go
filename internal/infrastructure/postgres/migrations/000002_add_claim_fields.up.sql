ALTER TABLE outbox_messages
    ADD COLUMN IF NOT EXISTS claimed_by TEXT NULL,
    ADD COLUMN IF NOT EXISTS claimed_until TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_error TEXT NULL;

CREATE INDEX IF NOT EXISTS idx_outbox_messages_claimed_until_occurred_at_id
    ON outbox_messages (claimed_until, occurred_at, id);
