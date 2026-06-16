package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/petretiandrea/outbox-go/internal/domain"
	"github.com/petretiandrea/outbox-go/pkg/outbox"
	outboxpg "github.com/petretiandrea/outbox-go/pkg/outbox/postgres"
)

const (
	defaultTableName    = "outbox_messages"
	defaultBatchSize    = 100
	defaultPollInterval = time.Second
)

type rowQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type SourceConfig struct {
	TableName    string
	BatchSize    int
	PollInterval time.Duration
}

type PollerSource struct {
	querier      rowQuerier
	tableName    string
	batchSize    int
	pollInterval time.Duration
	closeFn      func() error
}

func NewPollerSource(querier rowQuerier, config SourceConfig) (*PollerSource, error) {
	if querier == nil {
		return nil, errors.New("postgres source requires a row querier")
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

	return &PollerSource{
		querier:      querier,
		tableName:    tableName,
		batchSize:    batchSize,
		pollInterval: pollInterval,
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

	var cursorTime time.Time
	var cursorID string

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		messages, err := s.fetchBatch(ctx, cursorTime, cursorID)
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
			return err
		}

		last := messages[len(messages)-1]
		cursorTime = last.OccurredAt
		cursorID = last.ID
	}
}

func (s *PollerSource) Close() error {
	return s.closeFn()
}

func (s *PollerSource) fetchBatch(ctx context.Context, cursorTime time.Time, cursorID string) ([]outbox.Message, error) {
	query := fmt.Sprintf(`
SELECT id, channel, affinity_key, payload, metadata, occurred_at
FROM %s
WHERE occurred_at > $1 OR (occurred_at = $1 AND id > $2)
ORDER BY occurred_at ASC, id ASC
LIMIT $3
`, s.tableName)

	rows, err := s.querier.Query(ctx, query, cursorTime.UTC(), cursorID, s.batchSize)
	if err != nil {
		return nil, fmt.Errorf("query outbox messages: %w", err)
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
		return nil, fmt.Errorf("iterate outbox messages: %w", err)
	}

	return messages, nil
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

var _ domain.Source = (*PollerSource)(nil)
