package postgres

import (
	"context"
	"crypto/rand"
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

type claimedBatch struct {
	messages   []outbox.Message
	claimToken string
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
	if claimLease == 0 {
		claimLease = defaultClaimLease
	}
	if claimLease < time.Millisecond {
		return nil, errors.New("postgres source claim lease must be at least 1ms")
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

		claimed, err := s.claimBatch(ctx)
		if err != nil {
			return err
		}

		if len(claimed.messages) == 0 {
			timer := time.NewTimer(s.pollInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			continue
		}

		batch := make([]*outbox.Message, 0, len(claimed.messages))
		for idx := range claimed.messages {
			batch = append(batch, &claimed.messages[idx])
		}

		handlerErr, renewErr := s.handleWithRenewal(claimed, handler, batch)
		if handlerErr != nil {
			cleanupCtx, cancel := cleanupContext(s.claimLease)
			releaseErr := s.releaseMessages(cleanupCtx, claimed, handlerErr)
			cancel()
			if releaseErr != nil {
				return errors.Join(handlerErr, renewErr, releaseErr)
			}
			return errors.Join(handlerErr, renewErr)
		}

		if renewErr != nil {
			return renewErr
		}

		if err := s.deleteMessages(ctx, claimed); err != nil {
			return err
		}
	}
}

func (s *PollerSource) Close() error {
	return s.closeFn()
}

func (s *PollerSource) claimBatch(ctx context.Context) (claimedBatch, error) {
	claimToken, err := newClaimToken()
	if err != nil {
		return claimedBatch{}, err
	}

	records, err := s.queries.ClaimOutboxMessages(ctx, postgresqlc.ClaimOutboxMessagesParams{
		BatchSize:              int32(s.batchSize),
		ClaimedBy:              requiredText(s.claimOwner),
		ClaimToken:             requiredText(claimToken),
		ClaimLeaseMilliseconds: s.claimLease.Milliseconds(),
	})
	if err != nil {
		return claimedBatch{}, fmt.Errorf("claim outbox messages: %w", err)
	}

	messages := make([]outbox.Message, 0, len(records))
	for _, record := range records {
		message, err := toMessage(record)
		if err != nil {
			return claimedBatch{}, err
		}

		messages = append(messages, message)
	}

	return claimedBatch{messages: messages, claimToken: claimToken}, nil
}

func (s *PollerSource) deleteMessages(ctx context.Context, claimed claimedBatch) error {
	if len(claimed.messages) == 0 {
		return nil
	}

	deleted, err := s.queries.DeleteClaimedOutboxMessages(ctx, postgresqlc.DeleteClaimedOutboxMessagesParams{
		ClaimedBy:  requiredText(s.claimOwner),
		ClaimToken: requiredText(claimed.claimToken),
		Ids:        messageIDs(claimed.messages),
	})
	if err != nil {
		return fmt.Errorf("delete processed outbox messages: %w", err)
	}
	if deleted != int64(len(claimed.messages)) {
		return fmt.Errorf("delete processed outbox messages: deleted %d of %d claimed messages (claim was lost)", deleted, len(claimed.messages))
	}

	return nil
}

func (s *PollerSource) releaseMessages(ctx context.Context, claimed claimedBatch, cause error) error {
	if len(claimed.messages) == 0 {
		return nil
	}

	released, err := s.queries.ReleaseClaimedOutboxMessages(ctx, postgresqlc.ReleaseClaimedOutboxMessagesParams{
		LastError: requiredText(errorString(cause)),
		ClaimedBy: requiredText(s.claimOwner),
		ClaimToken: requiredText(claimed.claimToken),
		Ids:        messageIDs(claimed.messages),
	})
	if err != nil {
		return fmt.Errorf("release claimed outbox messages: %w", err)
	}
	if released != int64(len(claimed.messages)) {
		return fmt.Errorf("release claimed outbox messages: released %d of %d claimed messages (claim was lost)", released, len(claimed.messages))
	}

	return nil
}

func (s *PollerSource) handleWithRenewal(claimed claimedBatch, handler func(...*outbox.Message) error, batch []*outbox.Message) (error, error) {
	stop := make(chan struct{})
	workerDone := make(chan struct{})
	renewErr := make(chan error, 1)
	go func() {
		defer close(workerDone)
		interval := s.claimLease / 2
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				renewCtx, cancel := context.WithTimeout(context.Background(), s.claimLease)
				err := s.renewClaim(renewCtx, claimed)
				cancel()
				if err != nil {
					renewErr <- err
					return
				}
			case <-stop:
				return
			}
		}
	}()

	handlerErr := handler(batch...)
	close(stop)
	<-workerDone
	return handlerErr, receiveRenewalError(renewErr)
}

func (s *PollerSource) renewClaim(ctx context.Context, claimed claimedBatch) error {
	renewed, err := s.queries.RenewClaimedOutboxMessages(ctx, postgresqlc.RenewClaimedOutboxMessagesParams{
		ClaimLeaseMilliseconds: s.claimLease.Milliseconds(),
		ClaimedBy:              requiredText(s.claimOwner),
		ClaimToken:             requiredText(claimed.claimToken),
		Ids:                    messageIDs(claimed.messages),
	})
	if err != nil {
		return fmt.Errorf("renew claimed outbox messages: %w", err)
	}
	if renewed != int64(len(claimed.messages)) {
		return fmt.Errorf("renew claimed outbox messages: renewed %d of %d claimed messages (claim was lost)", renewed, len(claimed.messages))
	}
	return nil
}

func receiveRenewalError(errs <-chan error) error {
	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}

func cleanupContext(lease time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), lease)
}

func newClaimToken() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", fmt.Errorf("generate claim token: %w", err)
	}
	return fmt.Sprintf("%x", token), nil
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
