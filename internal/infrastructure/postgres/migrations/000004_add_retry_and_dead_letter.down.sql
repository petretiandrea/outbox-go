DROP TABLE IF EXISTS outbox_dead_letters;

ALTER TABLE outbox_messages
    DROP COLUMN IF EXISTS next_attempt_at;
