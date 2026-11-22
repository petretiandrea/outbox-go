package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/petretiandrea/outbox-go/pkg/outbox"
	outboxamqp "github.com/petretiandrea/outbox-go/pkg/outbox/amqp"
)

var (
	ErrPublishNack       = errors.New("rabbitmq broker negatively acknowledged publication")
	ErrUnroutableMessage = errors.New("rabbitmq publication was returned as unroutable")
	ErrConfirmClosed     = errors.New("rabbitmq publisher confirm stream closed")
	ErrReturnClosed      = errors.New("rabbitmq publisher return stream closed")
)

// amqpChannel is kept small so confirm and return handling can be tested without
// a broker. *amqp.Channel implements it.
type amqpChannel interface {
	Confirm(noWait bool) error
	NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation
	NotifyReturn(returned chan amqp.Return) chan amqp.Return
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Close() error
}

type amqpConnection interface {
	Close() error
}

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
	connection   amqpConnection
	channel      amqpChannel
	confirms     <-chan amqp.Confirmation
	returns      <-chan amqp.Return
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
	if config.DeliveryMode != 0 && config.DeliveryMode != outboxamqp.PersistentDeliveryMode {
		return nil, fmt.Errorf("rabbitmq publisher requires persistent delivery mode %d", outboxamqp.PersistentDeliveryMode)
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
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("enable rabbitmq publisher confirms: %w", err)
	}

	// Notifications are registered once and consumed while holding mu. This
	// serializes publications, making a returned message unambiguous.
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	returns := ch.NotifyReturn(make(chan amqp.Return, 1))

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
		// Mandatory routing is not optional: an unroutable message must fail the
		// publish so the outbox source will retain it for retry/remediation.
		mandatory:    true,
		immediate:    config.Immediate,
		confirms:     confirms,
		returns:      returns,
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

		if err := p.awaitConfirmation(ctx); err != nil {
			return fmt.Errorf("confirm rabbitmq publication %q: %w", message.ID, err)
		}
	}

	return nil
}

func (p *Publisher) awaitConfirmation(ctx context.Context) error {
	select {
	case returned, ok := <-p.returns:
		if !ok {
			return ErrReturnClosed
		}
		return fmt.Errorf("%w: reply code %d (%s), exchange %q, routing key %q", ErrUnroutableMessage, returned.ReplyCode, returned.ReplyText, returned.Exchange, returned.RoutingKey)
	case confirmation, ok := <-p.confirms:
		if !ok {
			return ErrConfirmClosed
		}
		if !confirmation.Ack {
			return ErrPublishNack
		}

		// RabbitMQ sends basic.return before the confirm for an unroutable
		// mandatory publication. Prefer that failure even when both buffered
		// notifications are already ready.
		select {
		case returned, ok := <-p.returns:
			if !ok {
				return ErrReturnClosed
			}
			return fmt.Errorf("%w: reply code %d (%s), exchange %q, routing key %q", ErrUnroutableMessage, returned.ReplyCode, returned.ReplyText, returned.Exchange, returned.RoutingKey)
		default:
			return nil
		}
	case <-ctx.Done():
		return ctx.Err()
	}
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
