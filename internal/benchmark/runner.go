package benchmark

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"petretiandrea.github.com/outbox/internal/domain"
	"petretiandrea.github.com/outbox/pkg/outbox"
)

type Config struct {
	Publisher        outbox.Publisher
	Source           domain.Source
	Output           io.Writer
	Channel          outbox.Channel
	Count            int
	BatchSize        int
	PayloadSize      int
	MeasureForwarder bool
}

func Run(ctx context.Context, config Config) error {
	if config.Publisher == nil {
		return errors.New("benchmark publisher is required")
	}
	if config.Output == nil {
		return errors.New("benchmark output is required")
	}
	if config.Channel == "" {
		return errors.New("channel is required")
	}
	if config.Count <= 0 {
		return errors.New("count must be greater than zero")
	}
	if config.BatchSize <= 0 {
		return errors.New("batch-size must be greater than zero")
	}
	if config.PayloadSize <= 0 {
		return errors.New("payload-size must be greater than zero")
	}
	payload := makePayload(config.PayloadSize)
	runID, err := randomID()
	if err != nil {
		return fmt.Errorf("generate benchmark run id: %w", err)
	}

	startedAt := time.Now()

	written := 0
	for written < config.Count {
		currentBatchSize := minInt(config.BatchSize, config.Count-written)
		messages := make([]outbox.Message, 0, currentBatchSize)

		for idx := 0; idx < currentBatchSize; idx++ {
			messageID, err := randomID()
			if err != nil {
				return fmt.Errorf("generate message id: %w", err)
			}

			message := outbox.NewMessageWithDefaults(
				messageID,
				config.Channel,
				outbox.Payload(payload),
			).WithMetadata("benchmark", "true").WithMetadata("benchmark_run_id", runID)

			messages = append(messages, message)
		}

		if err := config.Publisher.Publish(ctx, messages...); err != nil {
			return fmt.Errorf("publish batch starting at %d: %w", written, err)
		}

		written += currentBatchSize
	}

	elapsed := time.Since(startedAt)
	throughput := float64(config.Count) / elapsed.Seconds()

	fmt.Fprintf(config.Output, "wrote %d messages in %s\n", config.Count, elapsed.Round(time.Millisecond))
	fmt.Fprintf(config.Output, "throughput: %.2f msg/s\n", throughput)

	if !config.MeasureForwarder {
		return nil
	}

	if config.Source == nil {
		return errors.New("benchmark source is required")
	}

	mockPublisher := newPublisher(config.Channel, runID, config.Count)
	processor, err := domain.NewOutboxProcessor(domain.OutboxProcessorConfig{
		Source: config.Source,
		Publishers: map[outbox.Channel]outbox.Publisher{
			config.Channel: mockPublisher,
		},
	})
	if err != nil {
		return fmt.Errorf("create outbox processor: %w", err)
	}
	defer processor.Close()

	forwarderCtx, cancel := context.WithCancel(ctx)
	mockPublisher.setCancel(cancel)
	defer cancel()

	forwarderStartedAt := time.Now()
	err = processor.Process(forwarderCtx)
	forwarderElapsed := time.Since(forwarderStartedAt)

	if err != nil && !(mockPublisher.Done() && errors.Is(err, context.Canceled)) {
		return fmt.Errorf("run forwarder benchmark: %w", err)
	}

	forwarderThroughput := float64(config.Count) / forwarderElapsed.Seconds()
	fmt.Fprintf(config.Output, "forwarded %d messages in %s\n", config.Count, forwarderElapsed.Round(time.Millisecond))
	fmt.Fprintf(config.Output, "forwarder throughput: %.2f msg/s\n", forwarderThroughput)

	return nil
}

func makePayload(size int) []byte {
	payload := make([]byte, size)
	for idx := range payload {
		payload[idx] = byte('a' + (idx % 26))
	}
	return payload
}

func randomID() (string, error) {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return hex.EncodeToString(buffer), nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type publisher struct {
	channel     outbox.Channel
	runID       string
	targetCount int
	cancel      context.CancelFunc

	mu    sync.Mutex
	count int
	done  bool
}

func newPublisher(channel outbox.Channel, runID string, targetCount int) *publisher {
	return &publisher{
		channel:     channel,
		runID:       runID,
		targetCount: targetCount,
	}
}

func (p *publisher) setCancel(cancel context.CancelFunc) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cancel = cancel
}

func (p *publisher) Publish(ctx context.Context, messages ...outbox.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, message := range messages {
		if message.Channel != p.channel {
			continue
		}
		if message.Metadata["benchmark_run_id"] != p.runID {
			continue
		}

		p.count++
		if p.count >= p.targetCount && !p.done {
			p.done = true
			if p.cancel != nil {
				p.cancel()
			}
		}
	}

	return nil
}

func (p *publisher) Close() error {
	return nil
}

func (p *publisher) Done() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.done
}
