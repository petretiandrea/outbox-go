package amqp

import (
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/petretiandrea/outbox-go/pkg/outbox"
)

func TestNewPublishingMapsMessageToAMQP(t *testing.T) {
	occurredAt := time.Date(2026, 6, 22, 10, 30, 0, 0, time.UTC)
	message := outbox.Message{
		ID:          "order-123",
		Channel:     outbox.Channel("orders.created"),
		AffinityKey: outbox.AffinityKey("customer-456"),
		Payload:     outbox.Payload(`{"order_id":"order-123"}`),
		Metadata: outbox.Metadata{
			"content-type": "application/json",
		},
		OccurredAt: occurredAt,
	}

	publishing, err := NewPublishing(message, PublishingConfig{
		ContentType: "application/json",
	})
	if err != nil {
		t.Fatalf("NewPublishing returned error: %v", err)
	}

	if publishing.MessageId != message.ID {
		t.Fatalf("expected message id %q, got %q", message.ID, publishing.MessageId)
	}
	if publishing.Type != string(message.Channel) {
		t.Fatalf("expected type %q, got %q", message.Channel, publishing.Type)
	}
	if string(publishing.Body) != string(message.Payload) {
		t.Fatalf("expected body %q, got %q", message.Payload, publishing.Body)
	}
	if publishing.ContentType != "application/json" {
		t.Fatalf("expected content type application/json, got %q", publishing.ContentType)
	}
	if publishing.DeliveryMode != PersistentDeliveryMode {
		t.Fatalf("expected persistent delivery mode, got %d", publishing.DeliveryMode)
	}
	if publishing.Headers[AffinityKeyHeader] != string(message.AffinityKey) {
		t.Fatalf("expected affinity key header %q, got %v", message.AffinityKey, publishing.Headers[AffinityKeyHeader])
	}
}

func TestMessageFromDeliveryMapsAMQPToMessage(t *testing.T) {
	occurredAt := time.Date(2026, 6, 22, 10, 30, 0, 0, time.UTC)
	delivery := amqp091.Delivery{
		Headers: amqp091.Table{
			AffinityKeyHeader: "customer-456",
			"source":          "checkout",
		},
		Body:      []byte(`{"order_id":"order-123"}`),
		MessageId: "order-123",
		Timestamp: occurredAt,
		Type:      "orders.created",
	}

	message, err := MessageFromDelivery(delivery)
	if err != nil {
		t.Fatalf("MessageFromDelivery returned error: %v", err)
	}

	if message.ID != delivery.MessageId {
		t.Fatalf("expected message id %q, got %q", delivery.MessageId, message.ID)
	}
	if message.Channel != outbox.Channel(delivery.Type) {
		t.Fatalf("expected channel %q, got %q", delivery.Type, message.Channel)
	}
	if message.AffinityKey != "customer-456" {
		t.Fatalf("expected affinity key customer-456, got %q", message.AffinityKey)
	}
	if message.Metadata["source"] != "checkout" {
		t.Fatalf("expected metadata source checkout, got %q", message.Metadata["source"])
	}
	if _, ok := message.Metadata[AffinityKeyHeader]; ok {
		t.Fatalf("affinity key header should not be copied into metadata")
	}
}

func TestMessageFromDeliveryReturnsErrorForInvalidMessage(t *testing.T) {
	delivery := amqp091.Delivery{
		Body: []byte(`{"order_id":"order-123"}`),
		Type: "orders.created",
	}

	if _, err := MessageFromDelivery(delivery); err == nil {
		t.Fatal("expected invalid message error")
	}
}

func TestMessageFromDeliveryReturnsErrorForUnsupportedHeader(t *testing.T) {
	delivery := amqp091.Delivery{
		Headers: amqp091.Table{
			"attempt": int32(10),
		},
		Body:      []byte(`{"order_id":"order-123"}`),
		MessageId: "order-123",
		Timestamp: time.Date(2026, 6, 22, 10, 30, 0, 0, time.UTC),
		Type:      "orders.created",
	}

	if _, err := MessageFromDelivery(delivery); err == nil {
		t.Fatal("expected unsupported header error")
	}
}
