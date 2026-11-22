-- name: ClaimOutboxMessages :many
WITH candidate AS (
    SELECT id
    FROM outbox_messages
    WHERE dead_lettered_at IS NULL
      AND (failed_at IS NULL OR sqlc.arg(dead_letter_enabled)::boolean)
      AND (claimed_until IS NULL OR claimed_until <= now())
      AND (next_attempt_at IS NULL OR next_attempt_at <= now())
    ORDER BY occurred_at ASC, id ASC
    LIMIT sqlc.arg(batch_size)
    FOR UPDATE SKIP LOCKED
),
claimed AS (
    UPDATE outbox_messages AS outbox
    SET claimed_by = sqlc.arg(claimed_by),
        claim_token = sqlc.arg(claim_token),
        claimed_until = now() + (sqlc.arg(claim_lease_milliseconds)::bigint * interval '1 millisecond'),
        attempts = CASE WHEN outbox.failed_at IS NULL THEN outbox.attempts + 1 ELSE outbox.attempts END,
        last_error = CASE WHEN outbox.failed_at IS NULL THEN NULL ELSE outbox.last_error END
    FROM candidate
    WHERE outbox.id = candidate.id
    RETURNING outbox.id, outbox.channel, outbox.affinity_key, outbox.payload, outbox.metadata, outbox.occurred_at, outbox.attempts, outbox.failed_at, outbox.last_error, outbox.dead_letter_attempts
)
SELECT id, channel, affinity_key, payload, metadata, occurred_at, attempts, failed_at, last_error, dead_letter_attempts
FROM claimed
ORDER BY occurred_at ASC, id ASC;

-- name: DeleteClaimedOutboxMessages :execrows
DELETE FROM outbox_messages
WHERE claimed_by = sqlc.arg(claimed_by)
  AND claim_token = sqlc.arg(claim_token)
  AND claimed_until > now()
  AND id = ANY(sqlc.arg(ids)::text[]);

-- name: ReleaseClaimedOutboxMessages :execrows
-- Returns rows to the queue. next_attempt_at is now() plus an exponential
-- backoff derived from the attempts counter, capped at max_backoff. When
-- immediate is true (shutdown) the attempt is not counted and not delayed.
UPDATE outbox_messages AS outbox
SET claimed_by = NULL,
    claim_token = NULL,
    claimed_until = NULL,
    attempts = CASE WHEN sqlc.arg(immediate)::boolean THEN GREATEST(outbox.attempts - 1, 0) ELSE outbox.attempts END,
    next_attempt_at = CASE
        WHEN sqlc.arg(immediate)::boolean THEN NULL
        ELSE now() + LEAST(
            sqlc.arg(max_backoff_milliseconds)::bigint,
            sqlc.arg(base_backoff_milliseconds)::bigint * power(2, LEAST(GREATEST(outbox.attempts - 1, 0), 30))::bigint
        ) * interval '1 millisecond'
    END,
    last_error = failure.error
FROM unnest(sqlc.arg(ids)::text[], sqlc.arg(errors)::text[]) AS failure(id, error)
WHERE outbox.id = failure.id
  AND outbox.claimed_by = sqlc.arg(claimed_by)
  AND outbox.claim_token = sqlc.arg(claim_token)
  AND outbox.claimed_until > now();

-- name: RenewClaimedOutboxMessages :execrows
UPDATE outbox_messages
SET claimed_until = now() + (sqlc.arg(claim_lease_milliseconds)::bigint * interval '1 millisecond')
WHERE claimed_by = sqlc.arg(claimed_by)
  AND claim_token = sqlc.arg(claim_token)
  AND claimed_until > now()
  AND id = ANY(sqlc.arg(ids)::text[]);

-- name: ParkClaimedOutboxMessages :execrows
-- Dead-letters poison rows when no DLQ is configured: they leave the claim
-- queue and stay in the table with last_error and attempts for inspection.
-- To replay one, clear dead_lettered_at and failed_at and reset attempts.
UPDATE outbox_messages AS outbox
SET claimed_by = NULL,
    claim_token = NULL,
    claimed_until = NULL,
    next_attempt_at = NULL,
    failed_at = COALESCE(outbox.failed_at, now()),
    dead_lettered_at = now(),
    last_error = failure.error
FROM unnest(sqlc.arg(ids)::text[], sqlc.arg(errors)::text[]) AS failure(id, error)
WHERE outbox.id = failure.id
  AND outbox.claimed_by = sqlc.arg(claimed_by)
  AND outbox.claim_token = sqlc.arg(claim_token)
  AND outbox.claimed_until > now();

-- name: MarkFailedOutboxMessages :execrows
-- Durably records that a row failed permanently while keeping the claim, so
-- that a crash before the DLQ acknowledges it resumes at the DLQ step instead
-- of publishing to the original channel again.
UPDATE outbox_messages AS outbox
SET failed_at = COALESCE(outbox.failed_at, now()),
    last_error = failure.error
FROM unnest(sqlc.arg(ids)::text[], sqlc.arg(errors)::text[]) AS failure(id, error)
WHERE outbox.id = failure.id
  AND outbox.claimed_by = sqlc.arg(claimed_by)
  AND outbox.claim_token = sqlc.arg(claim_token)
  AND outbox.claimed_until > now();

-- name: CompleteDeadLetterOutboxMessages :execrows
-- Marks rows as dead-lettered. Called only after the DLQ acknowledged them.
UPDATE outbox_messages
SET claimed_by = NULL,
    claim_token = NULL,
    claimed_until = NULL,
    next_attempt_at = NULL,
    dead_lettered_at = now()
WHERE claimed_by = sqlc.arg(claimed_by)
  AND claim_token = sqlc.arg(claim_token)
  AND claimed_until > now()
  AND id = ANY(sqlc.arg(ids)::text[]);

-- name: ReleaseDeadLetterOutboxMessages :execrows
-- Returns rows whose DLQ publish failed to the queue with exponential backoff
-- based on dead_letter_attempts. last_error keeps the original failure.
UPDATE outbox_messages
SET claimed_by = NULL,
    claim_token = NULL,
    claimed_until = NULL,
    dead_letter_attempts = dead_letter_attempts + 1,
    next_attempt_at = now() + LEAST(
        sqlc.arg(max_backoff_milliseconds)::bigint,
        sqlc.arg(base_backoff_milliseconds)::bigint * power(2, LEAST(dead_letter_attempts, 30))::bigint
    ) * interval '1 millisecond'
WHERE claimed_by = sqlc.arg(claimed_by)
  AND claim_token = sqlc.arg(claim_token)
  AND claimed_until > now()
  AND id = ANY(sqlc.arg(ids)::text[]);

-- name: GetOutboxStats :one
-- Operational snapshot for alerting. oldest_claimable_age_seconds is the age of
-- the oldest row that could be claimed right now (0 when there is none).
SELECT
    COALESCE(EXTRACT(EPOCH FROM now() - (min(occurred_at) FILTER (
        WHERE dead_lettered_at IS NULL
          AND (claimed_until IS NULL OR claimed_until <= now())
          AND (next_attempt_at IS NULL OR next_attempt_at <= now())
    ))), 0)::float8 AS oldest_claimable_age_seconds,
    (count(*) FILTER (WHERE dead_lettered_at IS NOT NULL))::bigint AS dead_lettered_count,
    (count(*) FILTER (WHERE failed_at IS NOT NULL AND dead_lettered_at IS NULL))::bigint AS pending_dead_letter_count
FROM outbox_messages;
