ALTER TABLE outbox_messages
    DROP COLUMN IF EXISTS claim_token;
