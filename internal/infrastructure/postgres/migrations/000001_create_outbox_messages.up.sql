CREATE TABLE IF NOT EXISTS outbox_messages (
    id TEXT PRIMARY KEY,
    channel TEXT NOT NULL,
    affinity_key TEXT NULL,
    payload BYTEA NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL,
    claimed_by TEXT NULL,
    claimed_until TIMESTAMPTZ NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_outbox_messages_occurred_at_id
    ON outbox_messages (occurred_at, id);

CREATE INDEX IF NOT EXISTS idx_outbox_messages_channel_occurred_at_id
    ON outbox_messages (channel, occurred_at, id);

CREATE INDEX IF NOT EXISTS idx_outbox_messages_claimed_until_occurred_at_id
    ON outbox_messages (claimed_until, occurred_at, id);
