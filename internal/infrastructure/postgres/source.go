package postgres

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
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

	defaultMaxAttempts = 20
	defaultBaseBackoff = time.Second
	defaultMaxBackoff  = 5 * time.Minute

	maxStoredErrorLength = 2000
)

type SourceConfig struct {
	TableName    string
	BatchSize    int
	PollInterval time.Duration
	ClaimLease   time.Duration
	ClaimOwner   string

	// MaxAttempts bounds how many times a row is claimed before a transient
	// failure dead-letters it. 0 selects the default (20); a negative value
	// retries forever. Permanent failures are dead-lettered immediately.
	MaxAttempts int
	// RetryBackoff is the delay after the first failed attempt; it doubles on
	// every further attempt up to MaxRetryBackoff.
	RetryBackoff    time.Duration
	MaxRetryBackoff time.Duration
	// Logger receives retry and dead-letter events. Defaults to slog.Default().
	Logger *slog.Logger
	// Metrics receives dead-letter counters and periodic table statistics.
	// Optional.
	Metrics Metrics
	// MetricsInterval is how often statistics are queried. Default is 15s.
	MetricsInterval time.Duration
}

type PollerSource struct {
	queries      *postgresqlc.Queries
	batchSize    int
	pollInterval time.Duration
	claimLease   time.Duration
	claimOwner   string
	closeFn      func() error

	maxAttempts int // <= 0 means unlimited
	baseBackoff time.Duration
	maxBackoff  time.Duration
	logger      *slog.Logger

	metrics         Metrics
	metricsInterval time.Duration
	lastStats       time.Time
}

type claimedBatch struct {
	messages   []outbox.Message
	claimToken string
	attempts   map[string]int32
	// invalid holds claimed rows that cannot be converted into a message
	// (e.g. corrupt metadata). They are dead-lettered without being handled.
	invalid []domain.MessageFailure
}

func (c claimedBatch) claimedIDs() []string {
	ids := messageIDs(c.messages)
	for _, f := range c.invalid {
		ids = append(ids, f.ID)
	}
	return ids
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

	maxAttempts := config.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = defaultMaxAttempts
	}
	baseBackoff := config.RetryBackoff
	if baseBackoff < 0 {
		return nil, errors.New("postgres source retry backoff must not be negative")
	}
	if baseBackoff == 0 {
		baseBackoff = defaultBaseBackoff
	}
	maxBackoff := config.MaxRetryBackoff
	if maxBackoff < 0 {
		return nil, errors.New("postgres source max retry backoff must not be negative")
	}
	if maxBackoff == 0 {
		maxBackoff = defaultMaxBackoff
	}
	if maxBackoff < baseBackoff {
		return nil, errors.New("postgres source max retry backoff must not be below the retry backoff")
	}

	metricsInterval := config.MetricsInterval
	if metricsInterval <= 0 {
		metricsInterval = defaultMetricsInterval
	}

	return &PollerSource{
		maxAttempts:     maxAttempts,
		baseBackoff:     baseBackoff,
		maxBackoff:      maxBackoff,
		logger:          config.Logger,
		metrics:         config.Metrics,
		metricsInterval: metricsInterval,
		queries:         postgresqlc.New(db),
		batchSize:       batchSize,
		pollInterval:    pollInterval,
		claimLease:      claimLease,
		claimOwner:      claimOwner,
		closeFn:         func() error { return nil },
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

// Subscribe claims batches and hands them to handler until ctx is cancelled.
//
// Failures never stop the loop. A handler error is interpreted as follows:
//   - nil: every message of the batch was published and is deleted.
//   - *domain.BatchError: only the listed messages failed; the others are
//     deleted. Permanent failures are dead-lettered, transient ones are released
//     with exponential backoff and dead-lettered once MaxAttempts is exhausted.
//   - any other error: the whole batch failed transiently.
//
// Database errors while claiming or settling are logged and retried with
// backoff, so a temporary outage recovers without restarting the process.
func (s *PollerSource) Subscribe(ctx context.Context, handler func(...*outbox.Message) error) error {
	if handler == nil {
		return errors.New("handler is required")
	}

	consecutiveClaimErrors := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		s.reportStats(ctx)

		claimed, err := s.claimBatch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			consecutiveClaimErrors++
			s.log().Warn("outbox claim failed; retrying", "error", err, "consecutive_failures", consecutiveClaimErrors)
			if err := s.sleep(ctx, s.claimErrorDelay(consecutiveClaimErrors)); err != nil {
				return err
			}
			continue
		}
		consecutiveClaimErrors = 0

		if len(claimed.messages) == 0 && len(claimed.invalid) == 0 {
			if err := s.sleep(ctx, s.pollInterval); err != nil {
				return err
			}
			continue
		}

		var handlerErr, renewErr error
		if len(claimed.messages) > 0 {
			batch := make([]*outbox.Message, 0, len(claimed.messages))
			for idx := range claimed.messages {
				batch = append(batch, &claimed.messages[idx])
			}
			handlerErr, renewErr = s.handleWithRenewal(claimed, handler, batch)
		}
		if renewErr != nil {
			s.log().Warn("outbox claim renewal failed; the claim may have been lost", "error", renewErr)
		}

		s.settle(ctx, claimed, handlerErr)
	}
}

func (s *PollerSource) sleep(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		delay = time.Millisecond
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *PollerSource) claimErrorDelay(consecutive int) time.Duration {
	base := s.pollInterval
	if base <= 0 {
		base = defaultPollInterval
	}
	maxDelay := s.maxBackoff
	if maxDelay <= 0 {
		maxDelay = defaultMaxBackoff
	}
	delay := base
	for i := 1; i < consecutive && delay < maxDelay; i++ {
		delay *= 2
	}
	if delay > maxDelay {
		delay = maxDelay
	}
	return delay
}

func (s *PollerSource) log() *slog.Logger {
	if s.logger != nil {
		return s.logger
	}
	return slog.Default()
}

type failureDisposition struct {
	id  string
	err string
}

// settle records the outcome of a handled batch. Published messages are
// deleted; failed ones are dead-lettered or released. It uses a context that
// outlives ctx so that a shutdown still records what was already published.
// Errors are logged: rows whose completion could not be recorded simply become
// claimable again once their lease expires (at-least-once delivery).
func (s *PollerSource) settle(ctx context.Context, claimed claimedBatch, handlerErr error) {
	cleanupCtx, cancel := cleanupContext(s.claimLease)
	defer cancel()

	failures := make(map[string]domain.MessageFailure, len(claimed.invalid))
	for _, f := range claimed.invalid {
		failures[f.ID] = f
	}
	var batchErr *domain.BatchError
	switch {
	case handlerErr == nil:
	case errors.As(handlerErr, &batchErr):
		for _, f := range batchErr.Failures {
			failures[f.ID] = f
		}
	default:
		for _, m := range claimed.messages {
			failures[m.ID] = domain.MessageFailure{ID: m.ID, Err: handlerErr}
		}
	}

	var succeeded []outbox.Message
	for _, m := range claimed.messages {
		if _, failed := failures[m.ID]; !failed {
			succeeded = append(succeeded, m)
		}
	}
	if err := s.deleteMessages(cleanupCtx, claimedBatch{messages: succeeded, claimToken: claimed.claimToken}); err != nil {
		s.log().Error("outbox completion failed; published messages may be delivered again", "error", err)
	}

	var dead, retry, immediate []failureDisposition
	for _, id := range claimed.claimedIDs() {
		failure, failed := failures[id]
		if !failed {
			continue
		}
		text := truncateError(failure.Err)
		attempts := claimed.attempts[id]
		switch {
		case failure.Permanent():
			dead = append(dead, failureDisposition{id, text})
		case ctx.Err() != nil:
			// Shutdown: the failure says nothing about the message.
			immediate = append(immediate, failureDisposition{id, text})
		case s.maxAttempts > 0 && int(attempts) >= s.maxAttempts:
			dead = append(dead, failureDisposition{id, fmt.Sprintf("retry limit of %d attempts exceeded: %s", s.maxAttempts, text)})
		default:
			retry = append(retry, failureDisposition{id, text})
		}
	}

	if err := s.deadLetterMessages(cleanupCtx, claimed.claimToken, dead); err != nil {
		s.log().Error("outbox dead-lettering failed", "error", err)
	}
	if err := s.releaseMessages(cleanupCtx, claimed.claimToken, retry, false); err != nil {
		s.log().Error("outbox release failed", "error", err)
	}
	if err := s.releaseMessages(cleanupCtx, claimed.claimToken, immediate, true); err != nil {
		s.log().Error("outbox release failed", "error", err)
	}
}

func truncateError(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	if len(text) > maxStoredErrorLength {
		text = strings.ToValidUTF8(text[:maxStoredErrorLength], "") + "…"
	}
	return text
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

	claimed := claimedBatch{
		messages:   make([]outbox.Message, 0, len(records)),
		claimToken: claimToken,
		attempts:   make(map[string]int32, len(records)),
	}
	for _, record := range records {
		claimed.attempts[record.ID] = record.Attempts
		message, err := toMessage(record)
		if err != nil {
			claimed.invalid = append(claimed.invalid, domain.MessageFailure{ID: record.ID, Err: domain.Permanent(err)})
			continue
		}

		claimed.messages = append(claimed.messages, message)
	}

	return claimed, nil
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

func splitDispositions(items []failureDisposition) (ids, errs []string) {
	for _, item := range items {
		ids = append(ids, item.id)
		errs = append(errs, item.err)
	}
	return ids, errs
}

// releaseMessages returns failed rows to the queue with backoff, or right away
// without counting the attempt when immediate is set.
func (s *PollerSource) releaseMessages(ctx context.Context, claimToken string, items []failureDisposition, immediate bool) error {
	if len(items) == 0 {
		return nil
	}
	ids, errs := splitDispositions(items)
	for _, item := range items {
		s.log().Warn("outbox message failed; will be retried", "message_id", item.id, "error", item.err, "immediate", immediate)
	}

	released, err := s.queries.ReleaseClaimedOutboxMessages(ctx, postgresqlc.ReleaseClaimedOutboxMessagesParams{
		Immediate:               immediate,
		MaxBackoffMilliseconds:  s.maxBackoff.Milliseconds(),
		BaseBackoffMilliseconds: s.baseBackoff.Milliseconds(),
		Ids:                     ids,
		Errors:                  errs,
		ClaimedBy:               requiredText(s.claimOwner),
		ClaimToken:              requiredText(claimToken),
	})
	if err != nil {
		return fmt.Errorf("release claimed outbox messages: %w", err)
	}
	if released != int64(len(ids)) {
		return fmt.Errorf("release claimed outbox messages: released %d of %d claimed messages (claim was lost)", released, len(ids))
	}
	return nil
}

// deadLetterMessages moves rows that failed permanently (or exhausted max_attempts)
// out of the claim queue. They stay in the table, which is the dead-letter
// queue, with last_error and attempts for inspection and replay.
func (s *PollerSource) deadLetterMessages(ctx context.Context, claimToken string, items []failureDisposition) error {
	if len(items) == 0 {
		return nil
	}
	ids, errs := splitDispositions(items)
	for _, item := range items {
		s.log().Error("outbox message dead-lettered", "message_id", item.id, "error", item.err)
	}

	parked, err := s.queries.ParkClaimedOutboxMessages(ctx, postgresqlc.ParkClaimedOutboxMessagesParams{
		Ids:        ids,
		Errors:     errs,
		ClaimedBy:  requiredText(s.claimOwner),
		ClaimToken: requiredText(claimToken),
	})
	if err != nil {
		return fmt.Errorf("dead-letter outbox messages: %w", err)
	}
	if parked != int64(len(ids)) {
		return fmt.Errorf("dead-letter outbox messages: parked %d of %d claimed messages (claim was lost)", parked, len(ids))
	}
	s.reportDeadLettered(len(ids))
	return nil
}

func (s *PollerSource) reportDeadLettered(count int) {
	if s.metrics != nil && count > 0 {
		s.metrics.DeadLettered(count)
	}
}

// reportStats queries table statistics at most once per metricsInterval.
func (s *PollerSource) reportStats(ctx context.Context) {
	if s.metrics == nil || time.Since(s.lastStats) < s.metricsInterval {
		return
	}
	s.lastStats = time.Now()

	stats, err := s.queries.GetOutboxStats(ctx)
	if err != nil {
		if ctx.Err() == nil {
			s.log().Warn("outbox statistics query failed", "error", err)
		}
		return
	}
	s.metrics.Snapshot(Stats{
		OldestClaimableAge: time.Duration(stats.OldestClaimableAgeSeconds * float64(time.Second)),
		DeadLettered:       stats.DeadLetteredCount,
	})
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
		Ids:                    claimed.claimedIDs(),
	})
	if err != nil {
		return fmt.Errorf("renew claimed outbox messages: %w", err)
	}
	if renewed != int64(len(claimed.claimedIDs())) {
		return fmt.Errorf("renew claimed outbox messages: renewed %d of %d claimed messages (claim was lost)", renewed, len(claimed.claimedIDs()))
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
	message := outbox.Message{
		ID:         record.ID,
		Channel:    outbox.Channel(record.Channel),
		Payload:    outbox.Payload(record.Payload),
		OccurredAt: record.OccurredAt.Time.UTC(),
	}
	if record.AffinityKey.Valid {
		message.AffinityKey = outbox.AffinityKey(record.AffinityKey.String)
	}

	// On corrupt metadata the rest of the message is still returned so that it
	// can be dead-lettered with its payload.
	if len(record.Metadata) > 0 {
		var metadata outbox.Metadata
		if err := json.Unmarshal(record.Metadata, &metadata); err != nil {
			return message, fmt.Errorf("unmarshal metadata for message %q: %w", record.ID, err)
		}
		message.Metadata = metadata
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
