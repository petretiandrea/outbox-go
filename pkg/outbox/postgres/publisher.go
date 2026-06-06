package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"petretiandrea.github.com/outbox/pkg/outbox"
)

const defaultTableName = "outbox_messages"

type sqlExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

type Publisher struct {
	executor  sqlExecutor
	tableName string
	closeFn   func() error
}

type PublisherConfig struct {
	TableName string
}

func NewPublisher(db sqlExecutor, config PublisherConfig) (*Publisher, error) {
	if db == nil {
		return nil, errors.New("postgres publisher requires a database executor")
	}

	tableName := strings.TrimSpace(config.TableName)
	if tableName == "" {
		tableName = defaultTableName
	}

	publisher := &Publisher{
		executor:  db,
		tableName: tableName,
		closeFn:   func() error { return nil },
	}

	return publisher, nil
}

func NewPublisherFromPool(pool *pgxpool.Pool, config PublisherConfig) (*Publisher, error) {
	if pool == nil {
		return nil, errors.New("postgres publisher requires a pgx pool")
	}

	publisher, err := NewPublisher(pool, config)
	if err != nil {
		return nil, err
	}

	publisher.closeFn = func() error {
		pool.Close()
		return nil
	}

	return publisher, nil
}

func (p *Publisher) Publish(ctx context.Context, messages ...outbox.Message) error {
	if len(messages) == 0 {
		return nil
	}

	statement, args, err := p.buildBulkInsert(messages)
	if err != nil {
		return err
	}

	if _, err := p.executor.Exec(ctx, statement, args...); err != nil {
		return fmt.Errorf("insert outbox messages: %w", err)
	}

	return nil
}

func (p *Publisher) Close() error {
	return p.closeFn()
}

func (p *Publisher) buildBulkInsert(messages []outbox.Message) (string, []any, error) {
	var builder strings.Builder

	builder.WriteString("INSERT INTO ")
	builder.WriteString(p.tableName)
	builder.WriteString(" (id, channel, affinity_key, payload, metadata, occurred_at) VALUES ")

	// 6 are the number of fields in the postgresMessage struct, which corresponds to the number of columns in the insert statement.
	args := make([]any, 0, len(messages)*6)

	for idx, message := range messages {
		if err := message.Validate(); err != nil {
			return "", nil, err
		}

		record, err := newPostgresMessage(message)
		if err != nil {
			return "", nil, err
		}

		if idx > 0 {
			builder.WriteString(", ")
		}

		base := idx*6 + 1
		builder.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)", base, base+1, base+2, base+3, base+4, base+5))

		args = append(args,
			record.ID,
			record.Channel,
			record.AffinityKey,
			record.Payload,
			record.Metadata,
			record.OccurredAt,
		)
	}

	return builder.String(), args, nil
}

func newPostgresMessage(message outbox.Message) (MessageRecord, error) {
	metadata, err := json.Marshal(message.Metadata)
	if err != nil {
		return MessageRecord{}, fmt.Errorf("marshal metadata for message %q: %w", message.ID, err)
	}

	return MessageRecord{
		ID:          message.ID,
		Channel:     string(message.Channel),
		AffinityKey: nullableStringPtr(string(message.AffinityKey)),
		Payload:     []byte(message.Payload),
		Metadata:    metadata,
		OccurredAt:  message.OccurredAt.UTC(),
	}, nil
}

func nullableStringPtr(value string) *string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return &value
}

var _ outbox.Publisher = (*Publisher)(nil)
