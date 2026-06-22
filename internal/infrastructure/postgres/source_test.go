package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

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
		db:         executor,
		tableName:  "outbox_messages",
		batchSize:  10,
		claimOwner: "worker-1",
		claimLease: time.Minute,
	}

	messages, err := source.claimBatch(context.Background())
	if err != nil {
		t.Fatalf("claimBatch returned error: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("expected one message, got %d", len(messages))
	}
	if messages[0].ID != "message-1" {
		t.Fatalf("expected message id message-1, got %q", messages[0].ID)
	}
	if !strings.Contains(executor.querySQL, "FOR UPDATE SKIP LOCKED") {
		t.Fatalf("expected claim query to use SKIP LOCKED, got %s", executor.querySQL)
	}
	if executor.queryArgs[0] != 10 {
		t.Fatalf("expected batch size arg 10, got %v", executor.queryArgs[0])
	}
	if executor.queryArgs[1] != "worker-1" {
		t.Fatalf("expected claim owner arg worker-1, got %v", executor.queryArgs[1])
	}
	if executor.queryArgs[2] != int64(time.Minute/time.Millisecond) {
		t.Fatalf("expected claim lease milliseconds, got %v", executor.queryArgs[2])
	}
}

func TestDeleteMessagesDeletesOnlyClaimedMessages(t *testing.T) {
	executor := &fakeExecutor{commandTag: pgconn.NewCommandTag("DELETE 2")}
	source := &PollerSource{
		db:         executor,
		tableName:  "outbox_messages",
		claimOwner: "worker-1",
	}

	err := source.deleteMessages(context.Background(), []outbox.Message{
		{ID: "message-1"},
		{ID: "message-2"},
	})
	if err != nil {
		t.Fatalf("deleteMessages returned error: %v", err)
	}

	if !strings.Contains(executor.execSQL, "DELETE FROM outbox_messages") {
		t.Fatalf("expected delete query, got %s", executor.execSQL)
	}
	if !strings.Contains(executor.execSQL, "claimed_by = $1") {
		t.Fatalf("expected delete query to filter by claim owner, got %s", executor.execSQL)
	}
	if executor.execArgs[0] != "worker-1" {
		t.Fatalf("expected claim owner arg worker-1, got %v", executor.execArgs[0])
	}

	ids, ok := executor.execArgs[1].([]string)
	if !ok {
		t.Fatalf("expected []string ids arg, got %T", executor.execArgs[1])
	}
	if len(ids) != 2 || ids[0] != "message-1" || ids[1] != "message-2" {
		t.Fatalf("unexpected ids arg: %v", ids)
	}
}

func TestDeleteMessagesReturnsErrorWhenClaimWasLost(t *testing.T) {
	executor := &fakeExecutor{commandTag: pgconn.NewCommandTag("DELETE 0")}
	source := &PollerSource{
		db:         executor,
		tableName:  "outbox_messages",
		claimOwner: "worker-1",
	}

	err := source.deleteMessages(context.Background(), []outbox.Message{
		{ID: "message-1"},
	})
	if err == nil {
		t.Fatal("expected delete mismatch error")
	}
}

func TestReleaseMessagesClearsClaimAndStoresError(t *testing.T) {
	executor := &fakeExecutor{}
	source := &PollerSource{
		db:         executor,
		tableName:  "outbox_messages",
		claimOwner: "worker-1",
	}

	cause := errors.New("publish failed")
	err := source.releaseMessages(context.Background(), []outbox.Message{
		{ID: "message-1"},
	}, cause)
	if err != nil {
		t.Fatalf("releaseMessages returned error: %v", err)
	}

	if !strings.Contains(executor.execSQL, "claimed_by = NULL") {
		t.Fatalf("expected release query to clear claim, got %s", executor.execSQL)
	}
	if executor.execArgs[2] != cause.Error() {
		t.Fatalf("expected last_error arg %q, got %v", cause.Error(), executor.execArgs[2])
	}
}

type fakeExecutor struct {
	rows       pgx.Rows
	querySQL   string
	queryArgs  []any
	execSQL    string
	execArgs   []any
	commandTag pgconn.CommandTag
}

func (e *fakeExecutor) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	e.querySQL = sql
	e.queryArgs = args
	if e.rows == nil {
		return &fakeRows{}, nil
	}
	return e.rows, nil
}

func (e *fakeExecutor) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	e.execSQL = sql
	e.execArgs = args
	if e.commandTag.String() == "" {
		return pgconn.NewCommandTag("OK"), nil
	}
	return e.commandTag, nil
}

type rowRecord struct {
	id          string
	channel     string
	affinityKey *string
	payload     []byte
	metadata    []byte
	occurredAt  time.Time
}

type fakeRows struct {
	records []rowRecord
	idx     int
}

func (r *fakeRows) Close() {}

func (r *fakeRows) Err() error { return nil }

func (r *fakeRows) CommandTag() pgconn.CommandTag { return pgconn.NewCommandTag("SELECT") }

func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

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
	*(dest[2].(**string)) = record.affinityKey
	*(dest[3].(*[]byte)) = record.payload
	*(dest[4].(*[]byte)) = record.metadata
	*(dest[5].(*time.Time)) = record.occurredAt
	return nil
}

func (r *fakeRows) Values() ([]any, error) { return nil, nil }

func (r *fakeRows) RawValues() [][]byte { return nil }

func (r *fakeRows) Conn() *pgx.Conn { return nil }
