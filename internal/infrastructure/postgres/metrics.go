package postgres

import (
	"log/slog"
	"time"
)

// Stats is a periodic snapshot of the outbox table for alerting.
type Stats struct {
	// OldestClaimableAge is the age of the oldest row that could be claimed
	// now. A growing value means the forwarder is stuck or falling behind.
	OldestClaimableAge time.Duration
	// DeadLettered is the number of rows in the dead-letter table. Alert
	// when it is above zero or grows.
	DeadLettered int64
}

// Metrics receives dead-letter events and periodic snapshots. Implementations
// must be safe for concurrent use and must not block. Wire it to Prometheus,
// OpenTelemetry or any alerting system; the default implementation logs.
type Metrics interface {
	// DeadLettered reports rows that were moved from the outbox to the
	// dead-letter queue (the outbox_dead_letters table).
	DeadLettered(count int)
	// Snapshot reports the periodic table statistics.
	Snapshot(stats Stats)
}

const defaultMetricsInterval = 15 * time.Second

// NewLogMetrics returns a Metrics implementation that writes structured logs.
func NewLogMetrics(logger *slog.Logger) Metrics {
	return logMetrics{logger: logger}
}

type logMetrics struct{ logger *slog.Logger }

func (m logMetrics) log() *slog.Logger {
	if m.logger != nil {
		return m.logger
	}
	return slog.Default()
}

func (m logMetrics) DeadLettered(count int) {
	m.log().Info("outbox metric", "metric", "outbox_dead_lettered_total", "increment", count)
}

func (m logMetrics) Snapshot(stats Stats) {
	m.log().Info("outbox metric",
		"metric", "outbox_stats",
		"oldest_claimable_age_seconds", stats.OldestClaimableAge.Seconds(),
		"dead_lettered", stats.DeadLettered,
	)
}
