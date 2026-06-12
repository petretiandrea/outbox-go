package kafka

import (
	"context"
	"errors"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"petretiandrea.github.com/outbox/pkg/outbox"
)

type PublisherConfig struct {
	Brokers      []string
	Topic        string
	ClientID     string
	BatchBytes   int
	BatchSize    int
	BatchTimeout time.Duration
	Async        bool
}

type Publisher struct {
	writer *kafkago.Writer
}

func NewPublisher(config PublisherConfig) (*Publisher, error) {
	if len(config.Brokers) == 0 {
		return nil, errors.New("kafka publisher requires at least one broker")
	}
	if strings.TrimSpace(config.Topic) == "" {
		return nil, errors.New("kafka publisher requires a topic")
	}

	writer := &kafkago.Writer{
		Addr:         kafkago.TCP(config.Brokers...),
		Topic:        config.Topic,
		Async:        config.Async,
		BatchBytes:   int64(config.BatchBytes),
		BatchSize:    config.BatchSize,
		BatchTimeout: config.BatchTimeout,
	}
	if strings.TrimSpace(config.ClientID) != "" {
		writer.Transport = &kafkago.Transport{
			ClientID: config.ClientID,
		}
	}

	return &Publisher{writer: writer}, nil
}

func (p *Publisher) Publish(ctx context.Context, messages ...outbox.Message) error {
	if len(messages) == 0 {
		return nil
	}

	kafkaMessages := make([]kafkago.Message, 0, len(messages))
	for _, message := range messages {
		if err := message.Validate(); err != nil {
			return err
		}

		kafkaMessage := kafkago.Message{
			Key:   []byte(message.AffinityKey),
			Value: []byte(message.Payload),
			Time:  message.OccurredAt,
		}
		for key, value := range message.Metadata {
			kafkaMessage.Headers = append(kafkaMessage.Headers, kafkago.Header{
				Key:   key,
				Value: []byte(value),
			})
		}

		kafkaMessages = append(kafkaMessages, kafkaMessage)
	}

	return p.writer.WriteMessages(ctx, kafkaMessages...)
}

func (p *Publisher) Close() error {
	if p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

var _ outbox.Publisher = (*Publisher)(nil)
