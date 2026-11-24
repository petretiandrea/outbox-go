package domain

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/petretiandrea/outbox-go/pkg/outbox"
)

type OutboxProcessorConfig struct {
	Source     Source
	Publishers map[outbox.Channel]outbox.Publisher
}

type OutboxProcessor struct {
	source     Source
	publishers map[outbox.Channel]outbox.Publisher
}

func NewOutboxProcessor(config OutboxProcessorConfig) (*OutboxProcessor, error) {
	if config.Source == nil {
		return nil, errors.New("processor source is required")
	}
	if len(config.Publishers) == 0 {
		return nil, errors.New("processor requires at least one publisher")
	}

	publishers := make(map[outbox.Channel]outbox.Publisher, len(config.Publishers))
	for channel, publisher := range config.Publishers {
		if strings.TrimSpace(string(channel)) == "" {
			return nil, errors.New("publisher channel is required")
		}
		if publisher == nil {
			return nil, fmt.Errorf("publisher for channel %q is required", channel)
		}
		publishers[channel] = publisher
	}

	return &OutboxProcessor{source: config.Source, publishers: publishers}, nil
}

func (p *OutboxProcessor) Process(ctx context.Context) error {
	return p.source.Subscribe(ctx, func(messages ...*outbox.Message) error {
		return p.handleMessages(ctx, messages...)
	})
}

func (p *OutboxProcessor) Close() error {
	var closeErr error

	for channel, publisher := range p.publishers {
		if err := publisher.Close(); err != nil && closeErr == nil {
			closeErr = fmt.Errorf("close publisher for channel %q: %w", channel, err)
		}
	}

	if err := p.source.Close(); err != nil && closeErr == nil {
		closeErr = fmt.Errorf("close source: %w", err)
	}

	return closeErr
}

// handleMessages publishes each channel group independently. Messages that are
// invalid or target an unknown channel fail permanently without affecting the
// rest of the batch; a failing channel only fails its own messages. The
// returned *BatchError lists every failed message so the source can delete the
// published ones, dead-letter poison records and retry transient failures.
func (p *OutboxProcessor) handleMessages(ctx context.Context, messages ...*outbox.Message) error {
	grouped := make(map[outbox.Channel][]outbox.Message)
	var failures []MessageFailure

	for _, message := range messages {
		if message == nil {
			continue
		}
		if err := message.Validate(); err != nil {
			failures = append(failures, MessageFailure{ID: message.ID, Err: Permanent(err)})
			continue
		}

		publisher, ok := p.publishers[message.Channel]
		if !ok || publisher == nil {
			failures = append(failures, MessageFailure{
				ID:  message.ID,
				Err: Permanent(fmt.Errorf("no publisher configured for channel %q", message.Channel)),
			})
			continue
		}

		grouped[message.Channel] = append(grouped[message.Channel], *message)
	}

	for channel, batch := range grouped {
		err := p.publishers[channel].Publish(ctx, batch...)
		if err == nil {
			continue
		}
		err = fmt.Errorf("publish batch for channel %q: %w", channel, err)
		for _, message := range batch {
			failures = append(failures, MessageFailure{ID: message.ID, Err: err})
		}
	}

	if len(failures) == 0 {
		return nil
	}
	return &BatchError{Failures: failures}
}
