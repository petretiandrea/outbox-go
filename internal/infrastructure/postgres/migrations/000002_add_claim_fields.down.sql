DROP INDEX IF EXISTS idx_outbox_messages_claimed_until_occurred_at_id;

ALTER TABLE outbox_messages
    DROP COLUMN IF EXISTS last_error,
    DROP COLUMN IF EXISTS attempts,
    DROP COLUMN IF EXISTS claimed_until,
    DROP COLUMN IF EXISTS claimed_by;
