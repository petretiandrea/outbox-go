package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/petretiandrea/outbox-go/internal/domain"
	postgresqlc "github.com/petretiandrea/outbox-go/internal/infrastructure/postgres/sqlc"
	"github.com/petretiandrea/outbox-go/pkg/outbox"
)

const (
	defaultTableName    = "outbox_messages"
	defaultBatchSize    = 100
	defaultPollInterval = time.Second
	defaultClaimLease   = 30 * time.Second
)

type SourceConfig struct {
	TableName    string
	BatchSize    int
	PollInterval time.Duration
	ClaimLease   time.Duration
	ClaimOwner   string
}

type PollerSource struct {
	queries      *postgresqlc.Queries
	batchSize    int
	pollInterval time.Duration
	claimLease   time.Duration
	claimOwner   string
	closeFn      func() error
}

func NewPollerSource(db postgresqlc.DBTX, config SourceConfig) (*PollerSource, error) {
	if db == nil {
		return nil, errors.New("postgres source requires a query executor")
	}

	tableName := strings.TrimSpace(config.TableName)
	if tableName == "" {
		tableName = defaultTableName
	}
	if tableName != defaultTableName {
		return nil, fmt.Errorf("postgres source only supports table_name %q", defaultTableName)
	}

	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}
	if batchSize > math.MaxInt32 {
		return nil, fmt.Errorf("postgres source batch size must not exceed %d", math.MaxInt32)
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
		queries:      postgresqlc.New(db),
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
	records, err := s.queries.ClaimOutboxMessages(ctx, postgresqlc.ClaimOutboxMessagesParams{
		BatchSize:              int32(s.batchSize),
		ClaimedBy:              requiredText(s.claimOwner),
		ClaimLeaseMilliseconds: s.claimLease.Milliseconds(),
	})
	if err != nil {
		return nil, fmt.Errorf("claim outbox messages: %w", err)
	}

	messages := make([]outbox.Message, 0, len(records))
	for _, record := range records {
		message, err := toMessage(record)
		if err != nil {
			return nil, err
		}

		messages = append(messages, message)
	}

	return messages, nil
}

func (s *PollerSource) deleteMessages(ctx context.Context, messages []outbox.Message) error {
	if len(messages) == 0 {
		return nil
	}

	deleted, err := s.queries.DeleteClaimedOutboxMessages(ctx, postgresqlc.DeleteClaimedOutboxMessagesParams{
		ClaimedBy: requiredText(s.claimOwner),
		Ids:       messageIDs(messages),
	})
	if err != nil {
		return fmt.Errorf("delete processed outbox messages: %w", err)
	}
	if deleted != int64(len(messages)) {
		return fmt.Errorf("delete processed outbox messages: deleted %d of %d claimed messages", deleted, len(messages))
	}

	return nil
}

func (s *PollerSource) releaseMessages(ctx context.Context, messages []outbox.Message, cause error) error {
	if len(messages) == 0 {
		return nil
	}

	if err := s.queries.ReleaseClaimedOutboxMessages(ctx, postgresqlc.ReleaseClaimedOutboxMessagesParams{
		LastError: requiredText(errorString(cause)),
		ClaimedBy: requiredText(s.claimOwner),
		Ids:       messageIDs(messages),
	}); err != nil {
		return fmt.Errorf("release claimed outbox messages: %w", err)
	}

	return nil
}

func toMessage(record postgresqlc.ClaimOutboxMessagesRow) (outbox.Message, error) {
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
		OccurredAt: record.OccurredAt.Time.UTC(),
	}

	if record.AffinityKey.Valid {
		message.AffinityKey = outbox.AffinityKey(record.AffinityKey.String)
	}

	return message, nil
}

func requiredText(value string) pgtype.Text {
	return pgtype.Text{String: value, Valid: true}
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
