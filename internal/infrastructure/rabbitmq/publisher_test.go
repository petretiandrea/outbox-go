package rabbitmq

import (
	"context"
	"errors"
	"strings"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/petretiandrea/outbox-go/pkg/outbox"
)

func TestPublisherPublishRequiresBrokerAck(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		prepare   func(*fakeChannel)
		wantIs    error
		wantError string
	}{
		{
			name: "broker nack",
			prepare: func(ch *fakeChannel) { ch.confirms <- amqp.Confirmation{Ack: false} },
			wantIs:  ErrPublishNack,
		},
		{
			name: "unroutable return",
			prepare: func(ch *fakeChannel) {
				ch.returns <- amqp.Return{ReplyCode: 312, ReplyText: "NO_ROUTE", Exchange: "events", RoutingKey: "missing"}
			},
			wantIs:    ErrUnroutableMessage,
			wantError: "NO_ROUTE",
		},
		{
			name:    "closed confirm stream",
			prepare: func(ch *fakeChannel) { close(ch.confirms) },
			wantIs:  ErrConfirmClosed,
		},
		{
			name: "connection error while publishing",
			prepare: func(ch *fakeChannel) { ch.publishErr = errors.New("connection closed") },
			wantError: "connection closed",
		},
		{
			name: "confirmed publication",
			prepare: func(ch *fakeChannel) { ch.confirms <- amqp.Confirmation{Ack: true} },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ch := newFakeChannel()
			test.prepare(ch)
			publisher := testPublisher(ch)

			err := publisher.Publish(context.Background(), testMessage())
			if !ch.mandatory {
				t.Fatal("publish did not require mandatory routing")
			}
			if test.wantIs == nil && test.wantError == "" {
				if err != nil {
					t.Fatalf("Publish() error = %v", err)
				}
				return
			}
			if test.wantIs != nil && !errors.Is(err, test.wantIs) {
				t.Fatalf("Publish() error = %v, want errors.Is(..., %v)", err, test.wantIs)
			}
			if test.wantError != "" && !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("Publish() error = %q, want it to contain %q", err, test.wantError)
			}
		})
	}
}

func TestPublisherPublishFailsWhenContextEndsBeforeConfirm(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := testPublisher(newFakeChannel()).Publish(ctx, testMessage())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish() error = %v, want context.Canceled", err)
	}
}

func TestNewPublisherRejectsTransientDelivery(t *testing.T) {
	t.Parallel()

	_, err := NewPublisher(PublisherConfig{
		URL:          "amqp://not-used",
		RoutingKey:   "orders.created",
		DeliveryMode: 1,
	})
	if err == nil {
		t.Fatal("NewPublisher() error = nil, want persistent delivery mode validation error")
	}
	if !strings.Contains(err.Error(), "persistent delivery mode") {
		t.Fatalf("NewPublisher() error = %q, want persistent delivery mode error", err)
	}
}

func testPublisher(ch *fakeChannel) *Publisher {
	return &Publisher{
		channel:      ch,
		confirms:     ch.confirms,
		returns:      ch.returns,
		routingKey:   "orders.created",
		contentType:  "application/json",
		deliveryMode: 2,
		mandatory:    true,
	}
}

func testMessage() outbox.Message {
	return outbox.NewMessageWithDefaults("message-1", "orders.created", []byte(`{"order_id":"1"}`))
}

type fakeChannel struct {
	confirms   chan amqp.Confirmation
	returns    chan amqp.Return
	mandatory  bool
	publishErr error
}

func newFakeChannel() *fakeChannel {
	return &fakeChannel{
		confirms: make(chan amqp.Confirmation, 1),
		returns:  make(chan amqp.Return, 1),
	}
}

func (f *fakeChannel) Confirm(bool) error { return nil }

func (f *fakeChannel) NotifyPublish(chan amqp.Confirmation) chan amqp.Confirmation { return f.confirms }

func (f *fakeChannel) NotifyReturn(chan amqp.Return) chan amqp.Return { return f.returns }

func (f *fakeChannel) PublishWithContext(_ context.Context, _ string, _ string, mandatory bool, _ bool, _ amqp.Publishing) error {
	f.mandatory = mandatory
	return f.publishErr
}

func (f *fakeChannel) Close() error { return nil }
