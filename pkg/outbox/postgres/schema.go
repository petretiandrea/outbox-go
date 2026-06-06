package postgres

import (
	"time"
)

// Publisher implements the outbox.Publisher interface for PostgreSQL.
// It provides methods to publish messages to a PostgreSQL database and manage the connection lifecycle.
type MessageRecord struct {
	ID          string    `db:"id"`
	Channel     string    `db:"channel"`
	AffinityKey *string   `db:"affinity_key"`
	Payload     []byte    `db:"payload"`
	Metadata    []byte    `db:"metadata"`
	OccurredAt  time.Time `db:"occurred_at"`
}