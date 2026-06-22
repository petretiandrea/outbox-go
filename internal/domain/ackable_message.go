package domain

import "github.com/petretiandrea/outbox-go/pkg/outbox"

type ackableMessage struct {
	Message *outbox.Message

	ackChan  chan struct{}
	nackChan chan struct{}
}

func newAckableMessage(message *outbox.Message) *ackableMessage {
	return &ackableMessage{
		Message:  message,
		ackChan:  make(chan struct{}),
		nackChan: make(chan struct{}),
	}
}

func (am *ackableMessage) Ack() {
	am.ackChan <- struct{}{}
}

func (am *ackableMessage) Nack() {
	am.nackChan <- struct{}{}
}
