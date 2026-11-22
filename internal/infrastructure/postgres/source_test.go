package postgres

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/petretiandrea/outbox-go/internal/domain"
	postgresqlc "github.com/petretiandrea/outbox-go/internal/infrastructure/postgres/sqlc"
	"github.com/petretiandrea/outbox-go/pkg/outbox"
)

func TestNewPollerSourceDefaultsClaimConfig(t *testing.T) {
	source, err := NewPollerSource(&fakeExecutor{}, SourceConfig{})
	if err != nil {
		t.Fatalf("NewPollerSource returned error: %v", err)
	}

	if source.claimLease != defaultClaimLease {
		t.Fatalf("expected default claim lease %s, got %s", defaultClaimLease, source.claimLease)
	}
	if strings.TrimSpace(source.claimOwner) == "" {
		t.Fatal("expected default claim owner")
	}
}

func TestNewPollerSourceRejectsCustomTableName(t *testing.T) {
	_, err := NewPollerSource(&fakeExecutor{}, SourceConfig{TableName: "custom_outbox_messages"})
	if err == nil {
		t.Fatal("expected custom table name to be rejected")
	}
}

func TestNewPollerSourceRejectsBatchSizeOutsideSQLCIntegerRange(t *testing.T) {
	_, err := NewPollerSource(&fakeExecutor{}, SourceConfig{BatchSize: math.MaxInt32 + 1})
	if err == nil {
		t.Fatal("expected oversized batch size to be rejected")
	}
}

func TestNewPollerSourceRejectsClaimLeaseBelowDatabasePrecision(t *testing.T) {
	_, err := NewPollerSource(&fakeExecutor{}, SourceConfig{ClaimLease: time.Nanosecond})
	if err == nil {
		t.Fatal("expected sub-millisecond claim lease to be rejected")
	}
}

func TestClaimBatchClaimsAvailableMessages(t *testing.T) {
	occurredAt := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	executor := &fakeExecutor{
		rows: &fakeRows{
			records: []rowRecord{
				{
					id:         "message-1",
					channel:    "orders.created",
					payload:    []byte(`{"id":"order-1"}`),
					metadata:   []byte(`{"source":"checkout"}`),
					occurredAt: occurredAt,
				},
			},
		},
	}
	source := &PollerSource{
		queries:    postgresqlc.New(executor),
		batchSize:  10,
		claimOwner: "worker-1",
		claimLease: time.Minute,
	}

	claimed, err := source.claimBatch(context.Background())
	if err != nil {
		t.Fatalf("claimBatch returned error: %v", err)
	}

	if len(claimed.messages) != 1 {
		t.Fatalf("expected one message, got %d", len(claimed.messages))
	}
	if claimed.messages[0].ID != "message-1" {
		t.Fatalf("expected message id message-1, got %q", claimed.messages[0].ID)
	}
	if claimed.claimToken == "" {
		t.Fatal("expected a per-claim token")
	}
	if !strings.Contains(executor.querySQL, "FOR UPDATE SKIP LOCKED") {
		t.Fatalf("expected claim query to use SKIP LOCKED, got %s", executor.querySQL)
	}
	if !strings.Contains(executor.querySQL, "FROM claimed\nORDER BY occurred_at ASC, id ASC") {
		t.Fatalf("expected claimed rows to be returned in occurrence order, got %s", executor.querySQL)
	}
	if executor.queryArgs[0] != int32(10) {
		t.Fatalf("expected batch size arg 10, got %v", executor.queryArgs[0])
	}
	if executor.queryArgs[1] != (pgtype.Text{String: "worker-1", Valid: true}) {
		t.Fatalf("expected claim owner arg worker-1, got %v", executor.queryArgs[1])
	}
	if executor.queryArgs[2] == (pgtype.Text{}) {
		t.Fatal("expected a claim token argument")
	}
	if executor.queryArgs[3] != int64(time.Minute/time.Millisecond) {
		t.Fatalf("expected claim lease milliseconds, got %v", executor.queryArgs[3])
	}
}

func TestDeleteMessagesDeletesOnlyClaimedMessages(t *testing.T) {
	executor := &fakeExecutor{commandTag: pgconn.NewCommandTag("DELETE 2")}
	source := &PollerSource{
		queries:    postgresqlc.New(executor),
		claimOwner: "worker-1",
	}

	err := source.deleteMessages(context.Background(), claimedBatch{claimToken: "claim-1", messages: []outbox.Message{{ID: "message-1"}, {ID: "message-2"}}})
	if err != nil {
		t.Fatalf("deleteMessages returned error: %v", err)
	}

	if !strings.Contains(executor.execSQL, "DELETE FROM outbox_messages") {
		t.Fatalf("expected delete query, got %s", executor.execSQL)
	}
	if !strings.Contains(executor.execSQL, "claimed_by = $1") {
		t.Fatalf("expected delete query to filter by claim owner, got %s", executor.execSQL)
	}
	if executor.execArgs[0] != (pgtype.Text{String: "worker-1", Valid: true}) {
		t.Fatalf("expected claim owner arg worker-1, got %v", executor.execArgs[0])
	}

	if executor.execArgs[1] != (pgtype.Text{String: "claim-1", Valid: true}) {
		t.Fatalf("expected claim token arg claim-1, got %v", executor.execArgs[1])
	}

	ids, ok := executor.execArgs[2].([]string)
	if !ok {
		t.Fatalf("expected []string ids arg, got %T", executor.execArgs[1])
	}
	if len(ids) != 2 || ids[0] != "message-1" || ids[1] != "message-2" {
		t.Fatalf("unexpected ids arg: %v", ids)
	}
}

func TestDeleteMessagesRejectsStaleCompletionAfterExpiryAndReclaimWithReusedOwner(t *testing.T) {
	executor := &fakeExecutor{commandTag: pgconn.NewCommandTag("DELETE 0")}
	source := &PollerSource{
		queries: postgresqlc.New(executor),
		// Both workers deliberately use this same configured owner. A newer
		// claim with another token causes the old worker's DELETE to affect zero rows.
		claimOwner: "shared-owner",
	}

	err := source.deleteMessages(context.Background(), claimedBatch{claimToken: "expired-claim-token", messages: []outbox.Message{{ID: "message-1"}}})
	if err == nil {
		t.Fatal("expected stale completion to be rejected")
	}
	if !strings.Contains(executor.execSQL, "claim_token = $2") {
		t.Fatalf("expected completion to fence by token, got %s", executor.execSQL)
	}
}

func TestReleaseMessagesClearsClaimStoresErrorAndBacksOff(t *testing.T) {
	executor := &fakeExecutor{commandTag: pgconn.NewCommandTag("UPDATE 1")}
	source := &PollerSource{
		queries:     postgresqlc.New(executor),
		claimOwner:  "worker-1",
		baseBackoff: time.Second,
		maxBackoff:  time.Minute,
	}

	err := source.releaseMessages(context.Background(), "claim-1", []failureDisposition{{id: "message-1", err: "publish failed"}}, false)
	if err != nil {
		t.Fatalf("releaseMessages returned error: %v", err)
	}

	if !strings.Contains(executor.execSQL, "claimed_by = NULL") || !strings.Contains(executor.execSQL, "next_attempt_at") {
		t.Fatalf("expected release query to clear claim and schedule a retry, got %s", executor.execSQL)
	}
	if !strings.Contains(executor.execSQL, "outbox.claim_token = $7") {
		t.Fatalf("expected release to fence by token, got %s", executor.execSQL)
	}
	if executor.execArgs[0] != false || executor.execArgs[1] != int64(60000) || executor.execArgs[2] != int64(1000) {
		t.Fatalf("unexpected backoff args: %v", executor.execArgs[:3])
	}
	if errs, _ := executor.execArgs[4].([]string); len(errs) != 1 || errs[0] != "publish failed" {
		t.Fatalf("expected last_error arg, got %v", executor.execArgs[4])
	}
}

func TestReleaseMessagesRejectsLostClaim(t *testing.T) {
	executor := &fakeExecutor{commandTag: pgconn.NewCommandTag("UPDATE 0")}
	source := &PollerSource{queries: postgresqlc.New(executor), claimOwner: "shared-owner"}

	err := source.releaseMessages(context.Background(), "stale-token", []failureDisposition{{id: "message-1", err: "publish failed"}}, false)
	if err == nil {
		t.Fatal("expected stale release to be rejected")
	}
}

func TestRenewClaimRejectsStaleClaimWithReusedOwner(t *testing.T) {
	executor := &fakeExecutor{commandTag: pgconn.NewCommandTag("UPDATE 0")}
	source := &PollerSource{queries: postgresqlc.New(executor), claimOwner: "shared-owner", claimLease: time.Second}

	err := source.renewClaim(context.Background(), claimedBatch{claimToken: "stale-token", messages: []outbox.Message{{ID: "message-1"}}})
	if err == nil {
		t.Fatal("expected stale claim renewal to fail")
	}
	if !strings.Contains(executor.execSQL, "claim_token = $3") {
		t.Fatalf("expected renewal to fence by token, got %s", executor.execSQL)
	}
}

func TestHandleWithRenewalRenewsSlowHandler(t *testing.T) {
	executor := &fakeExecutor{commandTag: pgconn.NewCommandTag("UPDATE 1")}
	source := &PollerSource{queries: postgresqlc.New(executor), claimOwner: "worker-1", claimLease: 2 * time.Millisecond}
	claimed := claimedBatch{claimToken: "claim-1", messages: []outbox.Message{{ID: "message-1"}}}

	handlerErr, renewErr := source.handleWithRenewal(claimed, func(...*outbox.Message) error {
		time.Sleep(4 * time.Millisecond)
		return nil
	}, []*outbox.Message{{ID: "message-1"}})
	if handlerErr != nil || renewErr != nil {
		t.Fatalf("expected slow handler renewal to succeed, got handler=%v renewal=%v", handlerErr, renewErr)
	}
	if !strings.Contains(executor.execSQL, "SET claimed_until") {
		t.Fatalf("expected a renewal query, got %s", executor.execSQL)
	}
}

func TestSubscribeReleasesClaimAfterCancellationUsingCleanupContext(t *testing.T) {
	executor := &fakeExecutor{commandTag: pgconn.NewCommandTag("UPDATE 1"), rows: &fakeRows{records: []rowRecord{{
		id: "message-1", channel: "orders.created", occurredAt: time.Now(), attempts: 1,
	}}}}
	source := &PollerSource{
		queries: postgresqlc.New(executor), batchSize: 1, claimOwner: "shared-owner", claimLease: time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	err := source.Subscribe(ctx, func(...*outbox.Message) error {
		cancel()
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation error, got %v", err)
	}
	if executor.execCtx == nil || !executor.execContextWasLive {
		t.Fatal("expected release to use a live cleanup context after cancellation")
	}
	if !strings.Contains(executor.execSQL, "outbox.claim_token = $7") {
		t.Fatalf("expected release to fence by claim token, got %s", executor.execSQL)
	}
	if executor.execArgs[0] != true {
		t.Fatalf("expected shutdown release to be immediate, got %v", executor.execArgs[0])
	}
}

func rows(records ...rowRecord) *fakeRows { return &fakeRows{records: records} }

func record(id string, attempts int32) rowRecord {
	return rowRecord{id: id, channel: "orders.created", payload: []byte("p"), occurredAt: time.Now(), attempts: attempts}
}

func newTestSource(executor *fakeExecutor) *PollerSource {
	return &PollerSource{
		queries: postgresqlc.New(executor), batchSize: 10, claimOwner: "worker-1", claimLease: time.Second,
		pollInterval: time.Millisecond, maxAttempts: 3, baseBackoff: time.Second, maxBackoff: time.Minute,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func execsContaining(executor *fakeExecutor, fragment string) []execCall {
	var out []execCall
	for _, call := range executor.execs {
		if strings.Contains(call.sql, fragment) {
			out = append(out, call)
		}
	}
	return out
}

func TestSettleDeletesPublishedDeadLettersPoisonAndRetriesTransient(t *testing.T) {
	executor := &fakeExecutor{commandTag: pgconn.NewCommandTag("UPDATE 1")}
	source := newTestSource(executor)
	claimed := claimedBatch{
		claimToken: "claim-1",
		messages:   []outbox.Message{{ID: "ok"}, {ID: "poison"}, {ID: "flaky"}},
		attempts:   map[string]int32{"ok": 1, "poison": 1, "flaky": 1},
	}
	handlerErr := &domain.BatchError{Failures: []domain.MessageFailure{
		{ID: "poison", Err: domain.Permanent(errors.New("unknown channel"))},
		{ID: "flaky", Err: errors.New("broker unavailable")},
	}}

	source.settle(context.Background(), claimed, handlerErr)

	deletes := execsContaining(executor, "DELETE FROM outbox_messages")
	if len(deletes) != 1 || !reflect.DeepEqual(deletes[0].args[2], []string{"ok"}) {
		t.Fatalf("expected only the published message to be deleted, got %+v", deletes)
	}
	parked := execsContaining(executor, "-- name: ParkClaimedOutboxMessages")
	if len(parked) != 1 || !reflect.DeepEqual(parked[0].args[0], []string{"poison"}) {
		t.Fatalf("expected the poison message to be dead-lettered, got %+v", parked)
	}
	releases := execsContaining(executor, "next_attempt_at = CASE")
	if len(releases) != 1 || !reflect.DeepEqual(releases[0].args[3], []string{"flaky"}) || releases[0].args[0] != false {
		t.Fatalf("expected the transient failure to be released with backoff, got %+v", releases)
	}
}

func TestSettleDeadLettersTransientFailureAfterMaxAttempts(t *testing.T) {
	executor := &fakeExecutor{commandTag: pgconn.NewCommandTag("UPDATE 1")}
	source := newTestSource(executor)
	claimed := claimedBatch{
		claimToken: "claim-1",
		messages:   []outbox.Message{{ID: "exhausted"}},
		attempts:   map[string]int32{"exhausted": 3},
	}

	source.settle(context.Background(), claimed, errors.New("broker unavailable"))

	parked := execsContaining(executor, "-- name: ParkClaimedOutboxMessages")
	if len(parked) != 1 {
		t.Fatalf("expected exhausted message to be dead-lettered, got %+v", executor.execs)
	}
	if errs := parked[0].args[1].([]string); !strings.Contains(errs[0], "retry limit of 3 attempts exceeded") {
		t.Fatalf("expected retry limit reason, got %v", errs)
	}
	if len(execsContaining(executor, "next_attempt_at = CASE")) != 0 {
		t.Fatal("exhausted message must not be released")
	}
}

func TestSettleDeadLettersRowsThatCannotBeDecoded(t *testing.T) {
	executor := &fakeExecutor{commandTag: pgconn.NewCommandTag("UPDATE 1"), rows: rows(
		rowRecord{id: "corrupt", channel: "orders.created", payload: []byte("p"), metadata: []byte("{not json"), occurredAt: time.Now(), attempts: 1},
		record("healthy", 1),
	)}
	source := newTestSource(executor)

	claimed, err := source.claimBatch(context.Background())
	if err != nil {
		t.Fatalf("a corrupt row must not fail the claim: %v", err)
	}
	if len(claimed.messages) != 1 || claimed.messages[0].ID != "healthy" {
		t.Fatalf("expected only the healthy message to be handled, got %+v", claimed.messages)
	}
	if len(claimed.invalid) != 1 || !domain.IsPermanent(claimed.invalid[0].Err) {
		t.Fatalf("expected corrupt row to be a permanent failure, got %+v", claimed.invalid)
	}

	source.settle(context.Background(), claimed, nil)
	parked := execsContaining(executor, "-- name: ParkClaimedOutboxMessages")
	if len(parked) != 1 || !reflect.DeepEqual(parked[0].args[0], []string{"corrupt"}) {
		t.Fatalf("expected corrupt row to be dead-lettered, got %+v", parked)
	}
	deletes := execsContaining(executor, "DELETE FROM outbox_messages")
	if len(deletes) != 1 || !reflect.DeepEqual(deletes[0].args[2], []string{"healthy"}) {
		t.Fatalf("expected healthy row to be deleted, got %+v", deletes)
	}
}

func TestSubscribeKeepsRunningAfterClaimAndHandlerErrors(t *testing.T) {
	executor := &fakeExecutor{
		commandTag: pgconn.NewCommandTag("UPDATE 1"),
		queryResults: []queryResult{
			{err: errors.New("connection refused")},
			{rows: rows(record("flaky", 1))},
			{rows: rows(record("healthy", 1))},
		},
	}
	source := newTestSource(executor)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var seen []string
	err := source.Subscribe(ctx, func(messages ...*outbox.Message) error {
		seen = append(seen, messages[0].ID)
		if messages[0].ID == "flaky" {
			return errors.New("broker unavailable")
		}
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected only cancellation to stop the loop, got %v", err)
	}
	if !reflect.DeepEqual(seen, []string{"flaky", "healthy"}) {
		t.Fatalf("expected the loop to continue past errors, handled %v", seen)
	}
	if len(execsContaining(executor, "next_attempt_at = CASE")) == 0 {
		t.Fatal("expected the failed message to be released with backoff")
	}
	deletes := execsContaining(executor, "DELETE FROM outbox_messages")
	if len(deletes) != 1 || !reflect.DeepEqual(deletes[0].args[2], []string{"healthy"}) {
		t.Fatalf("expected healthy message to be deleted, got %+v", deletes)
	}
}

func TestNewPollerSourceValidatesRetryConfig(t *testing.T) {
	if _, err := NewPollerSource(&fakeExecutor{}, SourceConfig{RetryBackoff: time.Minute, MaxRetryBackoff: time.Second}); err == nil {
		t.Fatal("expected max backoff below base backoff to be rejected")
	}
	source, err := NewPollerSource(&fakeExecutor{}, SourceConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if source.maxAttempts != defaultMaxAttempts || source.baseBackoff != defaultBaseBackoff || source.maxBackoff != defaultMaxBackoff {
		t.Fatalf("unexpected retry defaults: %+v", source)
	}
}

type execCall struct {
	sql  string
	args []any
}

type queryResult struct {
	rows pgx.Rows
	err  error
}

type fakeExecutor struct {
	rows               pgx.Rows
	querySQL           string
	queryArgs          []any
	execSQL            string
	execArgs           []any
	execCtx            context.Context
	execContextWasLive bool
	commandTag         pgconn.CommandTag

	execs        []execCall
	queryResults []queryResult
}

func (e *fakeExecutor) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	e.querySQL = sql
	e.queryArgs = args
	if len(e.queryResults) > 0 {
		next := e.queryResults[0]
		e.queryResults = e.queryResults[1:]
		return next.rows, next.err
	}
	if e.rows == nil {
		return &fakeRows{}, nil
	}
	return e.rows, nil
}

func (e *fakeExecutor) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	e.execCtx = ctx
	e.execContextWasLive = ctx.Err() == nil
	e.execSQL = sql
	e.execArgs = args
	e.execs = append(e.execs, execCall{sql: sql, args: args})
	if e.commandTag.String() == "" {
		return pgconn.NewCommandTag("OK"), nil
	}
	return e.commandTag, nil
}

func (e *fakeExecutor) QueryRow(context.Context, string, ...any) pgx.Row { return nil }

type rowRecord struct {
	id          string
	channel     string
	affinityKey *string
	payload     []byte
	metadata    []byte
	occurredAt  time.Time
	attempts    int32
}

type fakeRows struct {
	records []rowRecord
	idx     int
}

func (r *fakeRows) Close() {}

func (r *fakeRows) Err() error { return nil }

func (r *fakeRows) CommandTag() pgconn.CommandTag { return pgconn.NewCommandTag("SELECT") }

func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *fakeRows) TypeMap() *pgtype.Map { return pgtype.NewMap() }

func (r *fakeRows) Next() bool {
	if r.idx >= len(r.records) {
		return false
	}
	r.idx++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	record := r.records[r.idx-1]
	*(dest[0].(*string)) = record.id
	*(dest[1].(*string)) = record.channel
	affinityKey := dest[2].(*pgtype.Text)
	if record.affinityKey != nil {
		affinityKey.String = *record.affinityKey
		affinityKey.Valid = true
	}
	*(dest[3].(*[]byte)) = record.payload
	*(dest[4].(*[]byte)) = record.metadata
	*(dest[5].(*pgtype.Timestamptz)) = pgtype.Timestamptz{Time: record.occurredAt, Valid: true}
	*(dest[6].(*int32)) = record.attempts
	return nil
}

func (r *fakeRows) Values() ([]any, error) { return nil, nil }

func (r *fakeRows) RawValues() [][]byte { return nil }

func (r *fakeRows) Conn() *pgx.Conn { return nil }

type recordingMetrics struct {
	deadLettered int
}

func (m *recordingMetrics) DeadLettered(count int) { m.deadLettered += count }
func (m *recordingMetrics) Snapshot(Stats)         {}

func TestSettleReportsDeadLetteredCountAndKeepsHealthyRowsFlowing(t *testing.T) {
	executor := &fakeExecutor{commandTag: pgconn.NewCommandTag("UPDATE 1")}
	source := newTestSource(executor)
	metrics := &recordingMetrics{}
	source.metrics = metrics
	claimed := claimedBatch{
		claimToken: "claim-1",
		messages:   []outbox.Message{{ID: "ok"}, {ID: "poison"}},
		attempts:   map[string]int32{"ok": 1, "poison": 2},
	}
	handlerErr := &domain.BatchError{Failures: []domain.MessageFailure{
		{ID: "poison", Err: domain.Permanent(errors.New("rejected by broker"))},
	}}

	source.settle(context.Background(), claimed, handlerErr)

	deadLettered := execsContaining(executor, "-- name: ParkClaimedOutboxMessages")
	if len(deadLettered) != 1 || !reflect.DeepEqual(deadLettered[0].args[0], []string{"poison"}) {
		t.Fatalf("expected the poison row to be dead-lettered in the table, got %+v", deadLettered)
	}
	if errs := deadLettered[0].args[1].([]string); !strings.Contains(errs[0], "rejected by broker") {
		t.Fatalf("expected the failure reason to be stored with the row, got %v", errs)
	}
	deletes := execsContaining(executor, "DELETE FROM outbox_messages")
	if len(deletes) != 1 || !reflect.DeepEqual(deletes[0].args[2], []string{"ok"}) {
		t.Fatalf("healthy rows must still be deleted, got %+v", deletes)
	}
	if metrics.deadLettered != 1 {
		t.Fatalf("expected one dead-lettered row to be reported, got %d", metrics.deadLettered)
	}
}

func TestSettleDoesNotReportDeadLetteredWhenClaimWasLost(t *testing.T) {
	executor := &fakeExecutor{commandTag: pgconn.NewCommandTag("UPDATE 0")}
	source := newTestSource(executor)
	metrics := &recordingMetrics{}
	source.metrics = metrics
	claimed := claimedBatch{
		claimToken: "claim-1",
		messages:   []outbox.Message{{ID: "poison"}},
		attempts:   map[string]int32{"poison": 1},
	}

	source.settle(context.Background(), claimed, &domain.BatchError{Failures: []domain.MessageFailure{
		{ID: "poison", Err: domain.Permanent(errors.New("rejected by broker"))},
	}})

	if metrics.deadLettered != 0 {
		t.Fatalf("a lost claim must not be reported as dead-lettered, got %d", metrics.deadLettered)
	}
}
