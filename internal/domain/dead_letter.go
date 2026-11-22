package domain

import (
	"context"
	"strconv"
	"time"

	"github.com/petretiandrea/outbox-go/pkg/outbox"
)

// Metadata keys added to every message published to the dead-letter channel.
// They override identically named keys of the original message metadata.
const (
	DeadLetterOriginalChannelKey = "x-outbox-original-channel"
	DeadLetterMessageIDKey       = "x-outbox-message-id"
	DeadLetterAttemptsKey        = "x-outbox-attempts"
	DeadLetterLastErrorKey       = "x-outbox-last-error"
	DeadLetterFailedAtKey        = "x-outbox-failed-at"
)

// DeadLetter is a message that cannot be published to its channel, together
// with the reason, ready to be routed to the dead-letter channel.
type DeadLetter struct {
	Message   outbox.Message
	Attempts  int
	LastError string
	FailedAt  time.Time
}

// DeadLetterHandler publishes dead letters and returns nil only once the
// destination acknowledged all of them.
type DeadLetterHandler func(ctx context.Context, letters ...DeadLetter) error

// DeadLetterSource is a Source that can hand permanently failed messages to a
// dead-letter handler instead of parking them. Once a handler is set, the
// source marks a row as dead-lettered only after the handler returns nil and
// retries with backoff otherwise.
type DeadLetterSource interface {
	Source
	SetDeadLetterHandler(handler DeadLetterHandler)
}

// toMessage returns the message to publish to the dead-letter channel. The ID,
// payload, affinity key and occurrence time are preserved so that consumers can
// deduplicate and replay it; failure details travel in the metadata.
func (d DeadLetter) toMessage(channel outbox.Channel) outbox.Message {
	metadata := make(outbox.Metadata, len(d.Message.Metadata)+5)
	for k, v := range d.Message.Metadata {
		metadata[k] = v
	}
	metadata[DeadLetterOriginalChannelKey] = string(d.Message.Channel)
	metadata[DeadLetterMessageIDKey] = d.Message.ID
	metadata[DeadLetterAttemptsKey] = strconv.Itoa(d.Attempts)
	metadata[DeadLetterLastErrorKey] = d.LastError
	metadata[DeadLetterFailedAtKey] = d.FailedAt.UTC().Format(time.RFC3339Nano)

	message := d.Message
	message.Channel = channel
	message.Metadata = metadata
	return message
}
