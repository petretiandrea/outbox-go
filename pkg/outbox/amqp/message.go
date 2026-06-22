package amqp

import (
	"strings"

	amqp091 "github.com/rabbitmq/amqp091-go"

	"github.com/petretiandrea/outbox-go/pkg/outbox"
)

const (
	// AffinityKeyHeader is the AMQP header used to carry outbox.Message.AffinityKey.
	AffinityKeyHeader = "affinity_key"

	// DefaultContentType is used when an AMQP publishing does not specify a content type.
	DefaultContentType = "application/octet-stream"

	// PersistentDeliveryMode is the AMQP delivery mode used for durable messages.
	PersistentDeliveryMode uint8 = 2
)

// PublishingConfig controls AMQP properties that are not part of outbox.Message.
type PublishingConfig struct {
	ContentType  string
	DeliveryMode uint8
}

// NewPublishing converts an outbox.Message to the AMQP schema used by the RabbitMQ publisher.
func NewPublishing(message outbox.Message, config PublishingConfig) (amqp091.Publishing, error) {
	if err := message.Validate(); err != nil {
		return amqp091.Publishing{}, err
	}

	contentType := strings.TrimSpace(config.ContentType)
	if contentType == "" {
		contentType = DefaultContentType
	}

	deliveryMode := config.DeliveryMode
	if deliveryMode == 0 {
		deliveryMode = PersistentDeliveryMode
	}

	headers := HeadersFromMetadata(message.Metadata)
	if message.AffinityKey != "" {
		headers[AffinityKeyHeader] = string(message.AffinityKey)
	}

	return amqp091.Publishing{
		Headers:       headers,
		ContentType:   contentType,
		Body:          []byte(message.Payload),
		MessageId:     message.ID,
		Timestamp:     message.OccurredAt,
		DeliveryMode:  deliveryMode,
		CorrelationId: message.ID,
		Type:          string(message.Channel),
	}, nil
}

// HeadersFromMetadata converts message metadata to AMQP headers.
func HeadersFromMetadata(metadata outbox.Metadata) amqp091.Table {
	headers := amqp091.Table{}
	for key, value := range metadata {
		headers[key] = value
	}
	return headers
}

// MessageFromDelivery converts a consumed AMQP delivery back to an outbox.Message.
func MessageFromDelivery(delivery amqp091.Delivery) outbox.Message {
	metadata, affinityKey := metadataAndAffinityKey(delivery.Headers)

	return outbox.Message{
		ID:          delivery.MessageId,
		Channel:     outbox.Channel(delivery.Type),
		AffinityKey: affinityKey,
		Payload:     outbox.Payload(delivery.Body),
		Metadata:    metadata,
		OccurredAt:  delivery.Timestamp,
	}
}

// MessageFromPublishing converts AMQP publishing properties back to an outbox.Message.
func MessageFromPublishing(publishing amqp091.Publishing) outbox.Message {
	metadata, affinityKey := metadataAndAffinityKey(publishing.Headers)

	return outbox.Message{
		ID:          publishing.MessageId,
		Channel:     outbox.Channel(publishing.Type),
		AffinityKey: affinityKey,
		Payload:     outbox.Payload(publishing.Body),
		Metadata:    metadata,
		OccurredAt:  publishing.Timestamp,
	}
}

func metadataAndAffinityKey(headers amqp091.Table) (outbox.Metadata, outbox.AffinityKey) {
	if len(headers) == 0 {
		return nil, ""
	}

	metadata := make(outbox.Metadata, len(headers))
	var affinityKey outbox.AffinityKey

	for key, value := range headers {
		headerValue, ok := headerString(value)
		if !ok {
			continue
		}
		if key == AffinityKeyHeader {
			affinityKey = outbox.AffinityKey(headerValue)
			continue
		}
		metadata[key] = headerValue
	}

	if len(metadata) == 0 {
		metadata = nil
	}

	return metadata, affinityKey
}

func headerString(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case []byte:
		return string(typed), true
	default:
		return "", false
	}
}
