DROP INDEX IF EXISTS idx_outbox_messages_claimed_until_occurred_at_id;
DROP INDEX IF EXISTS idx_outbox_messages_channel_occurred_at_id;
DROP INDEX IF EXISTS idx_outbox_messages_occurred_at_id;
DROP TABLE IF EXISTS outbox_messages;
