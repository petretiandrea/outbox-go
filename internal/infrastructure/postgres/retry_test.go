package postgres

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/petretiandrea/outbox-go/internal/domain"
	"github.com/petretiandrea/outbox-go/pkg/outbox"
)

type retryPublisher struct {
	calls   int
	publish func() error
}

func (p *retryPublisher) Publish(context.Context, ...outbox.Message) error {
	p.calls++
	return p.publish()
}
func (p *retryPublisher) Close() error { return nil }

type retryExecutor struct {
	record    rowRecord
	events    []string
	cancel    context.CancelFunc
	onRelease func()
}

func (e *retryExecutor) Query(context.Context, string, ...any) (pgx.Rows, error) {
	e.events = append(e.events, "claim")
	return &fakeRows{records: []rowRecord{e.record}}, nil
}
func (e *retryExecutor) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	if strings.Contains(sql, "DELETE FROM") {
		e.events = append(e.events, "delete")
		e.cancel()
		return pgconn.NewCommandTag("DELETE 1"), nil
	}
	e.events = append(e.events, "release")
	if e.onRelease != nil {
		e.onRelease()
	}
	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func TestProcessorRetriesReleasedBatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	db := &retryExecutor{record: rowRecord{id: "1", channel: "orders", payload: []byte(`{}`), occurredAt: time.Now()}, cancel: cancel}
	publisher := &retryPublisher{}
	publisher.publish = func() error {
		db.events = append(db.events, "publish")
		if publisher.calls == 1 {
			return errors.New("temporarily unavailable")
		}
		return nil
	}
	source, err := NewPollerSource(db, SourceConfig{})
	if err != nil {
		t.Fatal(err)
	}
	processor, err := domain.NewOutboxProcessor(domain.OutboxProcessorConfig{Source: source, Publishers: map[outbox.Channel]outbox.Publisher{"orders": publisher}})
	if err != nil {
		t.Fatal(err)
	}
	if err := processor.Process(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Process: %v", err)
	}
	want := []string{"claim", "publish", "release", "claim", "publish", "delete"}
	if !reflect.DeepEqual(db.events, want) {
		t.Fatalf("events = %v, want %v", db.events, want)
	}
}

func TestProcessorDoesNotRetryPermanentErrors(t *testing.T) {
	for _, tc := range []struct{ name, id, channel string }{
		{"validation", "", "orders"},
		{"missing publisher", "1", "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			db := &retryExecutor{record: rowRecord{id: tc.id, channel: tc.channel, payload: []byte(`{}`), occurredAt: time.Now()}, cancel: cancel}
			publisher := &retryPublisher{publish: func() error { t.Fatal("unexpected publish"); return nil }}
			source, err := NewPollerSource(db, SourceConfig{})
			if err != nil {
				t.Fatal(err)
			}
			processor, err := domain.NewOutboxProcessor(domain.OutboxProcessorConfig{Source: source, Publishers: map[outbox.Channel]outbox.Publisher{"orders": publisher}})
			if err != nil {
				t.Fatal(err)
			}
			if err := processor.Process(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("expected immediate permanent error, got %v", err)
			}
			if !reflect.DeepEqual(db.events, []string{"claim", "release"}) {
				t.Fatalf("unexpected events: %v", db.events)
			}
		})
	}
}

func TestProcessorRetryWaitCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db := &retryExecutor{record: rowRecord{id: "1", channel: "orders", payload: []byte(`{}`), occurredAt: time.Now()}, cancel: cancel, onRelease: cancel}
	publisher := &retryPublisher{publish: func() error { return errors.New("unavailable") }}
	source, err := NewPollerSource(db, SourceConfig{})
	if err != nil {
		t.Fatal(err)
	}
	processor, err := domain.NewOutboxProcessor(domain.OutboxProcessorConfig{Source: source, Publishers: map[outbox.Channel]outbox.Publisher{"orders": publisher}})
	if err != nil {
		t.Fatal(err)
	}
	if err := processor.Process(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Process: %v", err)
	}
	if publisher.calls != 1 || !reflect.DeepEqual(db.events, []string{"claim", "release"}) {
		t.Fatalf("unexpected retry: calls=%d events=%v", publisher.calls, db.events)
	}
}

func TestPublishBackoffGrowthCapAndReset(t *testing.T) {
	var backoff publishBackoff
	// Select the upper jitter bound to verify the exponential limits exactly.
	upper := func(n int64) int64 { return n - 1 }
	for i, want := range []time.Duration{
		100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond,
		800 * time.Millisecond, 1600 * time.Millisecond, 3200 * time.Millisecond,
		5 * time.Second, 5 * time.Second, 5 * time.Second,
	} {
		if got := backoff.next(upper); got != want {
			t.Fatalf("attempt %d: got %s, want %s", i+1, got, want)
		}
	}
	backoff.reset()
	if got := backoff.next(upper); got != publishRetryBase {
		t.Fatalf("after reset: got %s, want %s", got, publishRetryBase)
	}
}

func TestPublishBackoffJitterBounds(t *testing.T) {
	for _, fraction := range []int64{0, 1, 2} {
		var backoff publishBackoff
		for attempt := 0; attempt < 20; attempt++ {
			got := backoff.next(func(n int64) int64 { return (n - 1) * fraction / 2 })
			want := backoff.limit/2 + (backoff.limit-backoff.limit/2)*time.Duration(fraction)/2
			if got != want || got > publishRetryCap || got < backoff.limit/2 {
				t.Fatalf("attempt %d: jitter delay %s, want %s (limit %s)", attempt+1, got, want, backoff.limit)
			}
		}
	}
}
