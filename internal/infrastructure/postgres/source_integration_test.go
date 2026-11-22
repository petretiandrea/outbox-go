package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	migratepostgres "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/petretiandrea/outbox-go/internal/domain"
	"github.com/petretiandrea/outbox-go/pkg/outbox"
)

// These tests run against a real PostgreSQL. Set OUTBOX_TEST_DSN to a database
// the tests may freely create and drop schemas in, for example:
//
//	OUTBOX_TEST_DSN=postgres://postgres@127.0.0.1:5432/postgres go test ./internal/infrastructure/postgres
func integrationDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("OUTBOX_TEST_DSN")
	if dsn == "" {
		t.Skip("OUTBOX_TEST_DSN is not set; skipping PostgreSQL integration test")
	}
	return dsn
}

// freshSchema returns a DSN whose search_path is a new, empty schema.
func freshSchema(t *testing.T, dsn string) string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	name := fmt.Sprintf("outbox_it_%d", time.Now().UnixNano())
	if _, err := db.Exec("CREATE SCHEMA " + name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, err := sql.Open("pgx", dsn)
		if err == nil {
			_, _ = cleanup.Exec("DROP SCHEMA " + name + " CASCADE")
			cleanup.Close()
		}
	})
	sep := "?"
	if containsQuery(dsn) {
		sep = "&"
	}
	return dsn + sep + "search_path=" + name
}

func containsQuery(dsn string) bool {
	for _, c := range dsn {
		if c == '?' {
			return true
		}
	}
	return false
}

func newIntegrationSource(t *testing.T, dsn string, config SourceConfig) (*PollerSource, *pgxpool.Pool) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	if config.PollInterval == 0 {
		config.PollInterval = 10 * time.Millisecond
	}
	source, err := NewPollerSource(pool, config)
	if err != nil {
		t.Fatal(err)
	}
	return source, pool
}

func insertRow(t *testing.T, pool *pgxpool.Pool, id, channel string, occurred time.Time) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO outbox_messages (id, channel, payload, metadata, occurred_at) VALUES ($1, $2, '\x7b7d', '{"k":"v"}', $3)`,
		id, channel, occurred)
	if err != nil {
		t.Fatal(err)
	}
}

type countingMetrics struct {
	deadLettered atomic.Int64
	last         atomic.Pointer[Stats]
}

func (m *countingMetrics) DeadLettered(count int) { m.deadLettered.Add(int64(count)) }
func (m *countingMetrics) Snapshot(s Stats)       { m.last.Store(&s) }

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestIntegrationPoisonRowIsDeadLetteredInTheTableAndHealthyRowsKeepFlowing(t *testing.T) {
	dsn := freshSchema(t, integrationDSN(t))
	if err := initializeSchema(dsn, ""); err != nil {
		t.Fatal(err)
	}
	metrics := &countingMetrics{}
	source, pool := newIntegrationSource(t, dsn, SourceConfig{MaxAttempts: 3, RetryBackoff: time.Millisecond, MaxRetryBackoff: 5 * time.Millisecond, Metrics: metrics, MetricsInterval: time.Millisecond})

	now := time.Now().Add(-time.Minute)
	insertRow(t, pool, "poison", "unknown", now)
	insertRow(t, pool, "flaky", "orders", now.Add(time.Second))
	insertRow(t, pool, "healthy", "orders", now.Add(2*time.Second))

	var published atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- source.Subscribe(ctx, func(messages ...*outbox.Message) error {
			var failures []domain.MessageFailure
			for _, m := range messages {
				switch m.ID {
				case "poison":
					failures = append(failures, domain.MessageFailure{ID: m.ID, Err: domain.Permanent(errors.New("no publisher for channel unknown"))})
				case "flaky":
					failures = append(failures, domain.MessageFailure{ID: m.ID, Err: errors.New("broker down")})
				default:
					published.Add(1)
				}
			}
			if len(failures) > 0 {
				return &domain.BatchError{Failures: failures}
			}
			return nil
		})
	}()

	waitFor(t, "poison and flaky rows to be dead-lettered and healthy row deleted", func() bool {
		var n int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_messages WHERE dead_lettered_at IS NOT NULL`).Scan(&n)
		return n == 2 && published.Load() >= 1
	})
	cancel()
	<-done

	var attempts int
	var lastError string
	var claimedBy, claimToken *string
	err := pool.QueryRow(context.Background(), `SELECT attempts, last_error, claimed_by, claim_token FROM outbox_messages WHERE id = 'poison'`).Scan(&attempts, &lastError, &claimedBy, &claimToken)
	if err != nil {
		t.Fatal(err)
	}
	if lastError != "no publisher for channel unknown" || attempts != 1 || claimedBy != nil || claimToken != nil {
		t.Fatalf("unexpected poison row: attempts=%d last_error=%q claimed_by=%v token=%v", attempts, lastError, claimedBy, claimToken)
	}
	if err := pool.QueryRow(context.Background(), `SELECT attempts, last_error FROM outbox_messages WHERE id = 'flaky'`).Scan(&attempts, &lastError); err != nil {
		t.Fatal(err)
	}
	if attempts != 3 || lastError == "" {
		t.Fatalf("expected flaky row to be dead-lettered after 3 attempts, got attempts=%d last_error=%q", attempts, lastError)
	}
	var healthy int
	_ = pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox_messages WHERE id = 'healthy'`).Scan(&healthy)
	if healthy != 0 {
		t.Fatal("healthy row should have been deleted after publishing")
	}
	if got := metrics.deadLettered.Load(); got != 2 {
		t.Fatalf("expected 2 dead-lettered reported, got %d", got)
	}
	if s := metrics.last.Load(); s == nil || s.DeadLettered != 2 {
		t.Fatalf("expected a snapshot with 2 dead-lettered rows, got %+v", s)
	}
}

func TestIntegrationDeadLetteredRowsCanBeReplayedWithSQL(t *testing.T) {
	dsn := freshSchema(t, integrationDSN(t))
	if err := initializeSchema(dsn, ""); err != nil {
		t.Fatal(err)
	}
	source, pool := newIntegrationSource(t, dsn, SourceConfig{})
	insertRow(t, pool, "m1", "orders", time.Now().Add(-time.Hour))

	var fail atomic.Bool
	fail.Store(true)
	var published atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = source.Subscribe(ctx, func(messages ...*outbox.Message) error {
			if fail.Load() {
				return &domain.BatchError{Failures: []domain.MessageFailure{{ID: messages[0].ID, Err: domain.Permanent(errors.New("bad"))}}}
			}
			published.Add(int64(len(messages)))
			return nil
		})
	}()
	waitFor(t, "row to be dead-lettered", func() bool {
		var n int
		_ = pool.QueryRow(ctx, `SELECT count(*) FROM outbox_messages WHERE dead_lettered_at IS NOT NULL`).Scan(&n)
		return n == 1
	})

	fail.Store(false)
	// Replay statement documented in the README.
	if _, err := pool.Exec(ctx, `UPDATE outbox_messages SET dead_lettered_at = NULL, attempts = 0, next_attempt_at = NULL, last_error = NULL WHERE id = 'm1'`); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "replayed row to be published", func() bool { return published.Load() == 1 })
}

func TestIntegrationMigrationRenamesQuarantinedRowsOfAnExistingDatabase(t *testing.T) {
	dsn := freshSchema(t, integrationDSN(t))
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sourceDriver, err := iofs.New(migrationFiles, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	databaseDriver, err := migratepostgres.WithInstance(db, &migratepostgres.Config{MigrationsTable: migrationTableName})
	if err != nil {
		t.Fatal(err)
	}
	migrator, err := migrate.NewWithInstance("iofs", sourceDriver, "postgres", databaseDriver)
	if err != nil {
		t.Fatal(err)
	}
	if err := migrator.Migrate(4); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO outbox_messages (id, channel, payload, occurred_at, quarantined_at, last_error, attempts) VALUES ('q', 'c', '\x7b7d', now(), now(), 'boom', 4)`); err != nil {
		t.Fatal(err)
	}

	if err := migrator.Up(); err != nil {
		t.Fatalf("up to 000005: %v", err)
	}
	var lastError string
	if err := db.QueryRow(`SELECT last_error FROM outbox_messages WHERE id = 'q' AND dead_lettered_at IS NOT NULL`).Scan(&lastError); err != nil || lastError != "boom" {
		t.Fatalf("quarantined row must be dead-lettered after the migration: %v %q", err, lastError)
	}

	if err := migrator.Steps(-1); err != nil {
		t.Fatalf("down 000005: %v", err)
	}
	if err := db.QueryRow(`SELECT last_error FROM outbox_messages WHERE id = 'q' AND quarantined_at IS NOT NULL`).Scan(&lastError); err != nil {
		t.Fatalf("down migration must restore quarantined_at: %v", err)
	}
}
