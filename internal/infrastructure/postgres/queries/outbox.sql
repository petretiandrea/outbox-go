-- name: ClaimOutboxMessages :many
WITH candidate AS (
    SELECT id
    FROM outbox_messages
    WHERE claimed_until IS NULL OR claimed_until <= now()
    ORDER BY occurred_at ASC, id ASC
    LIMIT sqlc.arg(batch_size)
    FOR UPDATE SKIP LOCKED
),
claimed AS (
    UPDATE outbox_messages AS outbox
    SET claimed_by = sqlc.arg(claimed_by),
        claimed_until = now() + (sqlc.arg(claim_lease_milliseconds)::bigint * interval '1 millisecond'),
        attempts = attempts + 1,
        last_error = NULL
    FROM candidate
    WHERE outbox.id = candidate.id
    RETURNING outbox.id, outbox.channel, outbox.affinity_key, outbox.payload, outbox.metadata, outbox.occurred_at
)
SELECT id, channel, affinity_key, payload, metadata, occurred_at
FROM claimed
ORDER BY occurred_at ASC, id ASC;

-- name: DeleteClaimedOutboxMessages :execrows
DELETE FROM outbox_messages
WHERE claimed_by = sqlc.arg(claimed_by)
  AND id = ANY(sqlc.arg(ids)::text[]);

-- name: ReleaseClaimedOutboxMessages :exec
UPDATE outbox_messages
SET claimed_by = NULL,
    claimed_until = NULL,
    last_error = sqlc.arg(last_error)
WHERE claimed_by = sqlc.arg(claimed_by)
  AND id = ANY(sqlc.arg(ids)::text[]);
