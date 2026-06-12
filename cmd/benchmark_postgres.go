package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"petretiandrea.github.com/outbox/internal/benchmark"
	"petretiandrea.github.com/outbox/internal/infrastructure/postgres"
	"petretiandrea.github.com/outbox/pkg/outbox"
	outboxpostgres "petretiandrea.github.com/outbox/pkg/outbox/postgres"
)

func newPostgresBenchmarkCommand() *cobra.Command {
	var dsn string
	var tableName string
	var channel string
	var total int
	var batchSize int
	var payloadSize int
	var measureForwarder bool
	var truncateFirst bool
	var pollInterval time.Duration

	command := &cobra.Command{
		Use:   "postgres",
		Short: "Benchmark the PostgreSQL outbox implementation",
		RunE: func(cmd *cobra.Command, args []string) error {
			if strings.TrimSpace(dsn) == "" {
				return fmt.Errorf("dsn is required")
			}
			if strings.TrimSpace(channel) == "" {
				return fmt.Errorf("channel is required")
			}
			if total <= 0 {
				return fmt.Errorf("count must be greater than zero")
			}
			if batchSize <= 0 {
				return fmt.Errorf("batch-size must be greater than zero")
			}
			if payloadSize <= 0 {
				return fmt.Errorf("payload-size must be greater than zero")
			}
			if measureForwarder && !truncateFirst {
				return fmt.Errorf("measure-forwarder requires truncate-first to avoid reading unrelated rows")
			}

			ctx := cmd.Context()
			pool, err := pgxpool.New(ctx, dsn)
			if err != nil {
				return fmt.Errorf("create postgres pool: %w", err)
			}
			defer pool.Close()

			if truncateFirst {
				if err := truncatePostgresOutbox(ctx, pool, tableName); err != nil {
					return err
				}
			}

			publisher, err := outboxpostgres.NewPublisher(pool, outboxpostgres.PublisherConfig{
				TableName: tableName,
			})
			if err != nil {
				return err
			}
			defer publisher.Close()

			var source *postgres.PollerSource
			if measureForwarder {
				source, err = postgres.NewPollerSource(pool, postgres.SourceConfig{
					TableName:    tableName,
					BatchSize:    batchSize,
					PollInterval: pollInterval,
				})
				if err != nil {
					return fmt.Errorf("create postgres source: %w", err)
				}
				defer source.Close()
			}

			return benchmark.Run(ctx, benchmark.Config{
				Publisher:        publisher,
				Source:           source,
				Output:           cmd.OutOrStdout(),
				Channel:          outbox.Channel(channel),
				Count:            total,
				BatchSize:        batchSize,
				PayloadSize:      payloadSize,
				MeasureForwarder: measureForwarder,
			})
		},
	}

	command.Flags().StringVar(&dsn, "dsn", "", "PostgreSQL DSN")
	command.Flags().StringVar(&tableName, "table", "outbox_messages", "outbox table name")
	command.Flags().StringVar(&channel, "channel", "benchmark", "logical message channel")
	command.Flags().IntVar(&total, "count", 1000, "number of messages to write")
	command.Flags().IntVar(&batchSize, "batch-size", 100, "number of messages per publish call")
	command.Flags().IntVar(&payloadSize, "payload-size", 256, "payload size in bytes")
	command.Flags().BoolVar(&measureForwarder, "measure-forwarder", false, "measure the forwarder path using a mock publisher")
	command.Flags().BoolVar(&truncateFirst, "truncate-first", false, "truncate the outbox table before writing benchmark rows")
	command.Flags().DurationVar(&pollInterval, "poll-interval", 10*time.Millisecond, "poll interval used by the benchmark forwarder")

	return command
}

func truncatePostgresOutbox(ctx context.Context, pool *pgxpool.Pool, tableName string) error {
	if _, err := pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", tableName)); err != nil {
		return fmt.Errorf("truncate outbox table: %w", err)
	}

	return nil
}
