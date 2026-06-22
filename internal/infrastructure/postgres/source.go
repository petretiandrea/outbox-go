package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/petretiandrea/outbox-go/internal/domain"
	"github.com/petretiandrea/outbox-go/pkg/outbox"
	outboxpg "github.com/petretiandrea/outbox-go/pkg/outbox/postgres"
)

const (
	defaultTableName    = "outbox_messages"
	defaultBatchSize    = 100
	defaultPollInterval = time.Second
	defaultClaimLease   = 30 * time.Second
)

type queryExecutor interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

type SourceConfig struct {
	TableName    string
	BatchSize    int
	PollInterval time.Duration
	ClaimLease   time.Duration
	ClaimOwner   string
}

type PollerSource struct {
	db           queryExecutor
	tableName    string
	batchSize    int
	pollInterval time.Duration
	claimLease   time.Duration
	claimOwner   string
	closeFn      func() error
}

func NewPollerSource(db queryExecutor, config SourceConfig) (*PollerSource, error) {
	if db == nil {
		return nil, errors.New("postgres source requires a query executor")
	}

	tableName := strings.TrimSpace(config.TableName)
	if tableName == "" {
		tableName = defaultTableName
	}

	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	pollInterval := config.PollInterval
	if pollInterval <= 0 {
		pollInterval = defaultPollInterval
	}

	claimLease := config.ClaimLease
	if claimLease <= 0 {
		claimLease = defaultClaimLease
	}

	claimOwner := strings.TrimSpace(config.ClaimOwner)
	if claimOwner == "" {
		claimOwner = defaultClaimOwner()
	}

	return &PollerSource{
		db:           db,
		tableName:    tableName,
		batchSize:    batchSize,
		pollInterval: pollInterval,
		claimLease:   claimLease,
		claimOwner:   claimOwner,
		closeFn:      func() error { return nil },
	}, nil
}

func NewPollerSourceFromPool(pool *pgxpool.Pool, config SourceConfig) (*PollerSource, error) {
	if pool == nil {
		return nil, errors.New("postgres source requires a pgx pool")
	}

	source, err := NewPollerSource(pool, config)
	if err != nil {
		return nil, err
	}

	source.closeFn = func() error {
		pool.Close()
		return nil
	}

	return source, nil
}

func (s *PollerSource) Subscribe(ctx context.Context, handler func(...*outbox.Message) error) error {
	if handler == nil {
		return errors.New("handler is required")
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		messages, err := s.claimBatch(ctx)
		if err != nil {
			return err
		}

		if len(messages) == 0 {
			timer := time.NewTimer(s.pollInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			continue
		}

		batch := make([]*outbox.Message, 0, len(messages))
		for idx := range messages {
			batch = append(batch, &messages[idx])
		}

		if err := handler(batch...); err != nil {
			if releaseErr := s.releaseMessages(ctx, messages, err); releaseErr != nil {
				return errors.Join(err, releaseErr)
			}
			return err
		}

		if err := s.deleteMessages(ctx, messages); err != nil {
			return err
		}
	}
}

func (s *PollerSource) Close() error {
	return s.closeFn()
}

func (s *PollerSource) claimBatch(ctx context.Context) ([]outbox.Message, error) {
	query := fmt.Sprintf(`
WITH candidate AS (
    SELECT id
    FROM %s
    WHERE claimed_until IS NULL OR claimed_until <= now()
    ORDER BY occurred_at ASC, id ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
UPDATE %s AS outbox
SET claimed_by = $2,
    claimed_until = now() + ($3 * interval '1 millisecond'),
    attempts = attempts + 1,
    last_error = NULL
FROM candidate
WHERE outbox.id = candidate.id
RETURNING outbox.id, outbox.channel, outbox.affinity_key, outbox.payload, outbox.metadata, outbox.occurred_at
`, s.tableName, s.tableName)

	rows, err := s.db.Query(ctx, query, s.batchSize, s.claimOwner, s.claimLease.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("claim outbox messages: %w", err)
	}
	defer rows.Close()

	messages := make([]outbox.Message, 0, s.batchSize)
	for rows.Next() {
		record, err := scanPostgresMessage(rows)
		if err != nil {
			return nil, err
		}

		message, err := toMessage(record)
		if err != nil {
			return nil, err
		}

		messages = append(messages, message)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed outbox messages: %w", err)
	}

	return messages, nil
}

func (s *PollerSource) deleteMessages(ctx context.Context, messages []outbox.Message) error {
	if len(messages) == 0 {
		return nil
	}

	query := fmt.Sprintf(`
DELETE FROM %s
WHERE claimed_by = $1 AND id = ANY($2)
`, s.tableName)

	commandTag, err := s.db.Exec(ctx, query, s.claimOwner, messageIDs(messages))
	if err != nil {
		return fmt.Errorf("delete processed outbox messages: %w", err)
	}
	if commandTag.RowsAffected() != int64(len(messages)) {
		return fmt.Errorf("delete processed outbox messages: deleted %d of %d claimed messages", commandTag.RowsAffected(), len(messages))
	}

	return nil
}

func (s *PollerSource) releaseMessages(ctx context.Context, messages []outbox.Message, cause error) error {
	if len(messages) == 0 {
		return nil
	}

	query := fmt.Sprintf(`
UPDATE %s
SET claimed_by = NULL,
    claimed_until = NULL,
    last_error = $3
WHERE claimed_by = $1 AND id = ANY($2)
`, s.tableName)

	if _, err := s.db.Exec(ctx, query, s.claimOwner, messageIDs(messages), errorString(cause)); err != nil {
		return fmt.Errorf("release claimed outbox messages: %w", err)
	}

	return nil
}

func scanPostgresMessage(rows pgx.Rows) (outboxpg.MessageRecord, error) {
	var record outboxpg.MessageRecord
	if err := rows.Scan(
		&record.ID,
		&record.Channel,
		&record.AffinityKey,
		&record.Payload,
		&record.Metadata,
		&record.OccurredAt,
	); err != nil {
		return outboxpg.MessageRecord{}, fmt.Errorf("scan outbox message: %w", err)
	}

	return record, nil
}

func toMessage(record outboxpg.MessageRecord) (outbox.Message, error) {
	var metadata outbox.Metadata
	if len(record.Metadata) > 0 {
		if err := json.Unmarshal(record.Metadata, &metadata); err != nil {
			return outbox.Message{}, fmt.Errorf("unmarshal metadata for message %q: %w", record.ID, err)
		}
	}

	message := outbox.Message{
		ID:         record.ID,
		Channel:    outbox.Channel(record.Channel),
		Payload:    outbox.Payload(record.Payload),
		Metadata:   metadata,
		OccurredAt: record.OccurredAt.UTC(),
	}

	if record.AffinityKey != nil {
		message.AffinityKey = outbox.AffinityKey(*record.AffinityKey)
	}

	return message, nil
}

func messageIDs(messages []outbox.Message) []string {
	ids := make([]string, 0, len(messages))
	for _, message := range messages {
		ids = append(ids, message.ID)
	}
	return ids
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func defaultClaimOwner() string {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		hostname = "outbox"
	}
	return hostname + "-" + strconv.Itoa(os.Getpid())
}

var _ domain.Source = (*PollerSource)(nil)
