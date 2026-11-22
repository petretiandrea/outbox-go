package domain

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/petretiandrea/outbox-go/pkg/outbox"
)

type deadLetterSource struct {
	handler DeadLetterHandler
}

func (s *deadLetterSource) Subscribe(context.Context, func(...*outbox.Message) error) error { return nil }
func (s *deadLetterSource) Close() error { return nil }
func (s *deadLetterSource) SetDeadLetterHandler(h DeadLetterHandler) { s.handler = h }

type capturingPublisher struct {
	err      error
	messages []outbox.Message
}

func (p *capturingPublisher) Publish(_ context.Context, messages ...outbox.Message) error {
	if p.err != nil {
		return p.err
	}
	p.messages = append(p.messages, messages...)
	return nil
}

func (*capturingPublisher) Close() error { return nil }

func TestDeadLetterPublishesToDLQWithFailureMetadata(t *testing.T) {
	source := &deadLetterSource{}
	orders, dlq := &capturingPublisher{}, &capturingPublisher{}
	_, err := NewOutboxProcessor(OutboxProcessorConfig{
		Source:            source,
		Publishers:        map[outbox.Channel]outbox.Publisher{"orders": orders, "orders.dlq": dlq},
		DeadLetterChannel: "orders.dlq",
	})
	if err != nil {
		t.Fatal(err)
	}
	if source.handler == nil {
		t.Fatal("expected the processor to register the dead-letter handler")
	}

	failedAt := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	original := outbox.Message{
		ID: "m-1", Channel: "orders", AffinityKey: "customer-1", Payload: outbox.Payload("body"),
		Metadata: outbox.Metadata{"content-type": "application/json", DeadLetterLastErrorKey: "spoofed"}, OccurredAt: failedAt.Add(-time.Hour),
	}
	err = source.handler(context.Background(), DeadLetter{Message: original, Attempts: 4, LastError: "boom", FailedAt: failedAt})
	if err != nil {
		t.Fatal(err)
	}

	if len(orders.messages) != 0 || len(dlq.messages) != 1 {
		t.Fatalf("expected exactly one message on the DLQ only, got orders=%d dlq=%d", len(orders.messages), len(dlq.messages))
	}
	got := dlq.messages[0]
	want := map[string]string{
		"content-type":               "application/json",
		DeadLetterOriginalChannelKey: "orders",
		DeadLetterMessageIDKey:       "m-1",
		DeadLetterAttemptsKey:        "4",
		DeadLetterLastErrorKey:       "boom",
		DeadLetterFailedAtKey:        "2026-11-22T10:00:00Z",
	}
	for k, v := range want {
		if got.Metadata[k] != v {
			t.Errorf("metadata[%q] = %q, want %q", k, got.Metadata[k], v)
		}
	}
	if got.ID != "m-1" || got.Channel != "orders.dlq" || got.AffinityKey != "customer-1" || string(got.Payload) != "body" || !got.OccurredAt.Equal(original.OccurredAt) {
		t.Fatalf("expected id, payload and affinity key to be preserved, got %+v", got)
	}
	if original.Metadata[DeadLetterLastErrorKey] != "spoofed" || original.Channel != "orders" {
		t.Fatal("the original message must not be modified")
	}
}

func TestDeadLetterReturnsPublisherError(t *testing.T) {
	source := &deadLetterSource{}
	dlq := &capturingPublisher{err: errors.New("dlq down")}
	if _, err := NewOutboxProcessor(OutboxProcessorConfig{
		Source:            source,
		Publishers:        map[outbox.Channel]outbox.Publisher{"orders.dlq": dlq},
		DeadLetterChannel: "orders.dlq",
	}); err != nil {
		t.Fatal(err)
	}
	if err := source.handler(context.Background(), DeadLetter{Message: outbox.Message{ID: "m"}}); err == nil {
		t.Fatal("a failed DLQ publish must be reported so the source retries it")
	}
}

func TestDeadLetterChannelValidation(t *testing.T) {
	publishers := map[outbox.Channel]outbox.Publisher{"orders": &capturingPublisher{}}
	if _, err := NewOutboxProcessor(OutboxProcessorConfig{Source: &deadLetterSource{}, Publishers: publishers, DeadLetterChannel: "missing"}); err == nil {
		t.Fatal("expected an unknown dead-letter channel to be rejected")
	}
	if _, err := NewOutboxProcessor(OutboxProcessorConfig{Source: newSingleMessageSource(outbox.Message{}), Publishers: publishers, DeadLetterChannel: "orders"}); err == nil {
		t.Fatal("expected a source without dead-letter support to be rejected")
	}
	source := &deadLetterSource{}
	if _, err := NewOutboxProcessor(OutboxProcessorConfig{Source: source, Publishers: publishers}); err != nil || source.handler != nil {
		t.Fatalf("without a DLQ nothing must be registered (err=%v)", err)
	}
}
