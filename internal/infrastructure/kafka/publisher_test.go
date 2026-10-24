package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/petretiandrea/outbox-go/internal/config"
	"github.com/petretiandrea/outbox-go/pkg/outbox"
	kafkago "github.com/segmentio/kafka-go"
)

func TestNewPublisherRequiresAcknowledgedDelivery(t *testing.T) {
	publisher, err := NewPublisher(PublisherConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "events",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	if publisher.writer.Async {
		t.Fatal("publisher must wait for delivery before acknowledging outbox rows")
	}
	if publisher.writer.RequiredAcks != kafkago.RequireAll {
		t.Fatalf("expected acknowledgements from all in-sync replicas, got %v", publisher.writer.RequiredAcks)
	}

	balancer, ok := publisher.writer.Balancer.(*kafkago.Hash)
	if !ok {
		t.Fatalf("expected affinity-aware hash balancing, got %T", publisher.writer.Balancer)
	}
	message := kafkago.Message{Key: []byte("customer-123")}
	partition := balancer.Balance(message, 0, 1, 2, 3)
	for i := 0; i < 10; i++ {
		if got := balancer.Balance(message, 0, 1, 2, 3); got != partition {
			t.Fatalf("same affinity key moved from partition %d to %d", partition, got)
		}
	}
}

func TestNewPublisherRejectsAsync(t *testing.T) {
	publisher, err := NewPublisher(PublisherConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "events",
		Async:   true,
	})
	if err == nil || !strings.Contains(err.Error(), "async must be false") {
		t.Fatalf("expected actionable async configuration error, got %v", err)
	}
	if publisher != nil {
		t.Fatal("unsafe async configuration must not create a publisher")
	}
}

func TestBuildPublisherFromConfigRejectsAsync(t *testing.T) {
	publisher, err := BuildPublisherFromConfig(config.PublisherConfig{
		Data: map[string]any{
			"brokers": []string{"localhost:9092"},
			"topic":   "events",
			"async":   true,
		},
	})
	if err == nil {
		_ = publisher.Close()
		t.Fatal("expected async YAML/env configuration to fail startup")
	}
	if !strings.Contains(err.Error(), "async must be false") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPublishPropagatesCanceledDelivery(t *testing.T) {
	publisher, err := NewPublisher(PublisherConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "events",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	message := outbox.NewMessageWithDefaults("event-1", "events", outbox.Payload("body"))
	if err := publisher.Publish(ctx, message); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled delivery to propagate to the source, got %v", err)
	}
}
