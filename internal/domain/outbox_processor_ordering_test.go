package domain

import (
	"context"
	"testing"
	"time"

	"github.com/petretiandrea/outbox-go/pkg/outbox"
)

func TestWorkersDoNotSerializeMessagesWithTheSameAffinityKey(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	published := make(chan string, 2)

	first := newSingleMessageSource(outbox.Message{
		ID:          "first",
		Channel:     "orders.created",
		AffinityKey: "customer-1",
		Payload:     outbox.Payload(`first`),
		OccurredAt:  time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC),
	})
	second := newSingleMessageSource(outbox.Message{
		ID:          "second",
		Channel:     "orders.created",
		AffinityKey: "customer-1",
		Payload:     outbox.Payload(`second`),
		OccurredAt:  time.Date(2026, 10, 25, 12, 0, 1, 0, time.UTC),
	})

	firstProcessor := newOrderingProcessor(t, first, &controlledPublisher{
		firstStarted: firstStarted,
		releaseFirst: releaseFirst,
		published:    published,
	})
	secondProcessor := newOrderingProcessor(t, second, &controlledPublisher{published: published})

	firstDone := make(chan error, 1)
	go func() { firstDone <- firstProcessor.Process(context.Background()) }()
	<-firstStarted

	if err := secondProcessor.Process(context.Background()); err != nil {
		t.Fatalf("second worker returned error: %v", err)
	}
	if got := <-published; got != "second" {
		t.Fatalf("expected the later same-key message to publish while the first worker is blocked, got %q", got)
	}

	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first worker returned error: %v", err)
	}
	if got := <-published; got != "first" {
		t.Fatalf("expected the first worker to publish after release, got %q", got)
	}
}

func newOrderingProcessor(t *testing.T, source Source, publisher outbox.Publisher) *OutboxProcessor {
	t.Helper()
	processor, err := NewOutboxProcessor(OutboxProcessorConfig{
		Source: source,
		Publishers: map[outbox.Channel]outbox.Publisher{
			"orders.created": publisher,
		},
	})
	if err != nil {
		t.Fatalf("NewOutboxProcessor returned error: %v", err)
	}
	return processor
}

type singleMessageSource struct{ message outbox.Message }

func newSingleMessageSource(message outbox.Message) *singleMessageSource { return &singleMessageSource{message: message} }

func (s *singleMessageSource) Subscribe(_ context.Context, handler func(...*outbox.Message) error) error {
	return handler(&s.message)
}

func (*singleMessageSource) Close() error { return nil }

type controlledPublisher struct {
	firstStarted chan<- struct{}
	releaseFirst <-chan struct{}
	published    chan<- string
}

func (p *controlledPublisher) Publish(_ context.Context, messages ...outbox.Message) error {
	for _, message := range messages {
		if message.ID == "first" {
			close(p.firstStarted)
			<-p.releaseFirst
		}
		p.published <- message.ID
	}
	return nil
}

func (*controlledPublisher) Close() error { return nil }
