package domain

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/petretiandrea/outbox-go/pkg/outbox"
)

type recordingPublisher struct {
	err       error
	published []string
}

func (p *recordingPublisher) Publish(_ context.Context, messages ...outbox.Message) error {
	if p.err != nil {
		return p.err
	}
	for _, m := range messages {
		p.published = append(p.published, m.ID)
	}
	return nil
}

func (*recordingPublisher) Close() error { return nil }

func message(id string, channel outbox.Channel) *outbox.Message {
	return &outbox.Message{ID: id, Channel: channel, Payload: outbox.Payload("p"), OccurredAt: time.Now()}
}

func newFailureProcessor(t *testing.T, publishers map[outbox.Channel]outbox.Publisher) *OutboxProcessor {
	t.Helper()
	processor, err := NewOutboxProcessor(OutboxProcessorConfig{Source: newSingleMessageSource(outbox.Message{}), Publishers: publishers})
	if err != nil {
		t.Fatal(err)
	}
	return processor
}

func failureIDs(t *testing.T, err error) map[string]MessageFailure {
	t.Helper()
	var batchErr *BatchError
	if !errors.As(err, &batchErr) {
		t.Fatalf("expected *BatchError, got %v", err)
	}
	out := map[string]MessageFailure{}
	for _, f := range batchErr.Failures {
		out[f.ID] = f
	}
	return out
}

func TestPoisonMessagesDoNotBlockHealthyMessages(t *testing.T) {
	orders := &recordingPublisher{}
	processor := newFailureProcessor(t, map[outbox.Channel]outbox.Publisher{"orders": orders})

	err := processor.handleMessages(context.Background(),
		message("unknown", "no-such-channel"),
		message("healthy", "orders"),
		&outbox.Message{ID: "invalid", Channel: "orders"},
	)

	failures := failureIDs(t, err)
	if len(failures) != 2 || !failures["unknown"].Permanent() || !failures["invalid"].Permanent() {
		t.Fatalf("expected two permanent failures, got %+v", failures)
	}
	if len(orders.published) != 1 || orders.published[0] != "healthy" {
		t.Fatalf("expected the healthy message to be published, got %v", orders.published)
	}
}

func TestPartialChannelFailureOnlyFailsThatChannel(t *testing.T) {
	good := &recordingPublisher{}
	bad := &recordingPublisher{err: errors.New("broker unavailable")}
	processor := newFailureProcessor(t, map[outbox.Channel]outbox.Publisher{"good": good, "bad": bad})

	err := processor.handleMessages(context.Background(),
		message("g1", "good"), message("b1", "bad"), message("g2", "good"), message("b2", "bad"))

	failures := failureIDs(t, err)
	var ids []string
	for id, f := range failures {
		if f.Permanent() {
			t.Fatalf("publisher errors must be transient by default: %s", id)
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if len(ids) != 2 || ids[0] != "b1" || ids[1] != "b2" {
		t.Fatalf("expected only the failing channel's messages to fail, got %v", ids)
	}
	if len(good.published) != 2 {
		t.Fatalf("expected the successful channel to be published, got %v", good.published)
	}
}

func TestPermanentPublisherErrorIsPreserved(t *testing.T) {
	pub := &recordingPublisher{err: Permanent(errors.New("message too large"))}
	processor := newFailureProcessor(t, map[outbox.Channel]outbox.Publisher{"orders": pub})

	failures := failureIDs(t, processor.handleMessages(context.Background(), message("m1", "orders")))
	if !failures["m1"].Permanent() {
		t.Fatal("expected a publisher-marked permanent error to stay permanent")
	}
}

func TestAllPublishedReturnsNil(t *testing.T) {
	processor := newFailureProcessor(t, map[outbox.Channel]outbox.Publisher{"orders": &recordingPublisher{}})
	if err := processor.handleMessages(context.Background(), message("m1", "orders")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
