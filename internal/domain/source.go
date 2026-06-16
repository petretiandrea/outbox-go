package domain

import (
	"context"

	"github.com/petretiandrea/outbox-go/pkg/outbox"
)

// Source represents a message source that can stream outbox messages to a handler.
type Source interface {
	Subscribe(ctx context.Context, handler func(...*outbox.Message) error) error
	Close() error
}
