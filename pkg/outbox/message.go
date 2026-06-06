package outbox

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Channel represents the destination where the message should be published.
type Channel string

// AffinityKey is used to determine the partition or shard for the message, ensuring that messages with the same key are processed in order.
type AffinityKey string

// Payload represents the data that needs to be published to the channel.
type Payload []byte

// Metadata is a key-value pair that can hold additional information about the message, such as headers or attributes.
type Metadata map[string]string

type Message struct {
	ID          string
	Channel     Channel
	AffinityKey AffinityKey
	Payload     Payload
	Metadata    Metadata
	OccurredAt  time.Time
}


func NewMessageWithDefaults(id string, channel Channel, payload Payload) Message {
	return Message{
		ID:          id,
		Channel:     channel,
		AffinityKey: "",
		Payload:     payload,
		Metadata:    nil,
		OccurredAt:  time.Now(),
	}
}

func NewMessage(id string, channel Channel, affinityKey AffinityKey, payload Payload, metadata Metadata) Message {
	return Message{
		ID:          id,
		Channel:     channel,
		AffinityKey: affinityKey,
		Payload:     payload,
		Metadata:    metadata,
		OccurredAt:  time.Now(),
	}
}

func (m Message) WithMetadata(key, value string) Message {
	if m.Metadata == nil {
		m.Metadata = make(Metadata)
	}
	m.Metadata[key] = value
	return m
}

func (m Message) Validate() error {
	switch {
	case strings.TrimSpace(m.ID) == "":
		return errors.New("message id is required")
	case strings.TrimSpace(string(m.Channel)) == "":
		return fmt.Errorf("message %q channel is required", m.ID)
	case m.Payload == nil:
		return fmt.Errorf("message %q payload is required", m.ID)
	case m.OccurredAt.IsZero():
		return fmt.Errorf("message %q occurred_at is required", m.ID)
	default:
		return nil
	}
}