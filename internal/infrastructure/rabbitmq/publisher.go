package rabbitmq

import (
	"context"
	"errors"
	"strings"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/petretiandrea/outbox-go/pkg/outbox"
	outboxamqp "github.com/petretiandrea/outbox-go/pkg/outbox/amqp"
)

type PublisherConfig struct {
	URL          string
	Exchange     string
	RoutingKey   string
	ContentType  string
	DeliveryMode uint8
	Mandatory    bool
	Immediate    bool
}

type Publisher struct {
	connection   *amqp.Connection
	channel      *amqp.Channel
	exchange     string
	routingKey   string
	contentType  string
	deliveryMode uint8
	mandatory    bool
	immediate    bool
	mu           sync.Mutex
}

func NewPublisher(config PublisherConfig) (*Publisher, error) {
	if strings.TrimSpace(config.URL) == "" {
		return nil, errors.New("rabbitmq publisher requires a url")
	}
	if strings.TrimSpace(config.RoutingKey) == "" {
		return nil, errors.New("rabbitmq publisher requires a routing key")
	}

	conn, err := amqp.Dial(config.URL)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	contentType := strings.TrimSpace(config.ContentType)
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	deliveryMode := config.DeliveryMode
	if deliveryMode == 0 {
		deliveryMode = outboxamqp.PersistentDeliveryMode
	}

	return &Publisher{
		connection:   conn,
		channel:      ch,
		exchange:     config.Exchange,
		routingKey:   config.RoutingKey,
		contentType:  contentType,
		deliveryMode: deliveryMode,
		mandatory:    config.Mandatory,
		immediate:    config.Immediate,
	}, nil
}

func (p *Publisher) Publish(ctx context.Context, messages ...outbox.Message) error {
	if len(messages) == 0 {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, message := range messages {
		publishing, err := outboxamqp.NewPublishing(message, outboxamqp.PublishingConfig{
			ContentType:  p.contentType,
			DeliveryMode: p.deliveryMode,
		})
		if err != nil {
			return err
		}

		if err := p.channel.PublishWithContext(
			ctx,
			p.exchange,
			p.routingKey,
			p.mandatory,
			p.immediate,
			publishing,
		); err != nil {
			return err
		}
	}

	return nil
}

func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var closeErr error
	if p.channel != nil {
		if err := p.channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) && closeErr == nil {
			closeErr = err
		}
	}
	if p.connection != nil {
		if err := p.connection.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) && closeErr == nil {
			closeErr = err
		}
	}

	return closeErr
}

var _ outbox.Publisher = (*Publisher)(nil)
