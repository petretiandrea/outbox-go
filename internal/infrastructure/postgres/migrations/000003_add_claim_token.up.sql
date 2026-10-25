-- Kept as a separate migration so installations already at migration 2 gain
-- claim fencing as well.  The IF NOT EXISTS also makes fresh installs safe
-- while migration 2 is retained for backwards compatibility.
ALTER TABLE outbox_messages
    ADD COLUMN IF NOT EXISTS claim_token TEXT NULL;
