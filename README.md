# Outbox Forwarder

Outbox Forwarder is a small service that reads messages from a PostgreSQL outbox table and publishes them to configured destination channels.

The service is configured with YAML and/or environment variables. The normal container command is already configured in the image:

```text
outbox run --config /etc/outbox/outbox.yaml
```

If you mount a config file at `/etc/outbox/outbox.yaml`, you do not need to pass container args.

## Publisher-Side Go Library

Applications that create business events can write messages to the PostgreSQL outbox table with the publisher package:

```bash
go get github.com/petretiandrea/outbox-go/pkg/outbox
```

### Versioning

The forwarder and publisher library are separate Go modules in this repository. Root releases use tags such as `v1.7.1`; publisher-library releases use the matching nested-module tag, for example `pkg/outbox/v1.7.1`. The release workflow publishes both tags together. To backfill a previously published root release, run the **Build outbox forwarder image** workflow manually and provide its root tag.

Import the core package and the PostgreSQL publisher:

```go
import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/petretiandrea/outbox-go/pkg/outbox"
	outboxpostgres "github.com/petretiandrea/outbox-go/pkg/outbox/postgres"
)
```

Minimal example with a `pgxpool.Pool`:

```go
func publishOrderCreated(ctx context.Context, pool *pgxpool.Pool) error {
	publisher, err := outboxpostgres.NewPublisher(pool, outboxpostgres.PublisherConfig{
		TableName: "outbox_messages",
	})
	if err != nil {
		return err
	}

	msg := outbox.NewMessage(
		"order-123",
		outbox.Channel("orders.created"),
		outbox.AffinityKey("customer-456"),
		outbox.Payload(`{"order_id":"order-123"}`),
		outbox.Metadata{"content-type": "application/json"},
	)

	return publisher.Publish(ctx, msg)
}
```

For transactional writes, create the publisher with the same `pgx.Tx` used by your business operation:

```go
publisher, err := outboxpostgres.NewPublisher(tx, outboxpostgres.PublisherConfig{})
```

This keeps the business data and outbox message committed together.

## Configuration Model

The config has two top-level sections:

```yaml
source:
  type: postgres
  data:
    # source-specific settings

channels:
  - name: some.logical.channel
    publisher:
      type: rabbitmq
      data:
        # publisher-specific settings
```

`source` tells the forwarder where to read outbox messages from.

`channels` maps outbox message channels to publisher implementations. Each channel name must match the `channel` value stored in the outbox message.

## Minimal PostgreSQL Source

```yaml
source:
  type: postgres
  data:
    dsn: postgres://outbox:outbox@postgres:5432/outbox?sslmode=disable
    table_name: outbox_messages
    batch_size: 100
    poll_interval: 1s
    claim_lease: 30s
    initialize_schema: true
```

Fields:

| Field | Required | Description |
| --- | --- | --- |
| `dsn` | yes | PostgreSQL connection string. |
| `table_name` | no | Must be `outbox_messages`; this is the default. |
| `batch_size` | no | Max messages fetched per poll. Default is `100`. |
| `poll_interval` | no | Delay when no messages are found. Default is `1s`. |
| `claim_lease` | no | How long a forwarder owns a claimed batch before another forwarder can retry it. Default is `30s`; positive values below `1ms` are rejected. |
| `claim_owner` | no | Identifier stored in claimed rows. Default is generated from hostname and process id. Every acquired batch also receives a unique internal claim token, so owners may safely be reused. |
| `max_attempts` | no | Claims allowed before a repeatedly failing message is dead-lettered. Default is `20`; a negative value retries forever. |
| `retry_backoff` | no | Delay before the first retry of a failed message; doubles on each further attempt. Default is `1s`. |
| `max_retry_backoff` | no | Upper bound for the retry delay. Default is `5m`; must not be below `retry_backoff`. |
| `initialize_schema` | no | If `true`, runs embedded PostgreSQL migrations during startup before subscribing. Default is `false`. |

`initialize_schema` is executed before the first subscribe loop. If schema initialization fails, the service fails fast and does not start processing.

The PostgreSQL source and embedded schema initialization use the `outbox_messages` table.

The PostgreSQL source claims rows before publishing and deletes them only after the publish succeeds. Each claim has a random token; delete, release, and renewal require both the configured owner and that token. The source renews a claim while its handler is running. If publishing fails, the claim is released with its token and the row is retried with backoff (see below). If the process crashes after publishing but before delete, or a lease renewal cannot reach PostgreSQL, the message can still be published again after the lease expires. Consumers must therefore treat delivery as at-least-once and use `Message.ID` for idempotency.

### Failures, retries and dead letters

A failure never stops the forwarder, and one bad row never blocks healthy rows:

- **Per-message outcomes.** Messages are published per channel. Rows whose channel published successfully are deleted even if another channel in the same batch failed; only the failed rows are kept. A crash between publishing and deleting can still republish a row, so delivery stays at-least-once.
- **Transient failures** (broker unavailable, timeouts, any publisher error not marked permanent) release the row with exponential backoff (`retry_backoff` up to `max_retry_backoff`, tracked in `next_attempt_at`). Other rows are claimed in the meantime. When the broker recovers the forwarder resumes by itself; no restart is needed. Database errors while claiming or completing are logged and retried with backoff too.
- **Permanent failures** (invalid message, unknown channel, metadata that cannot be decoded) are dead-lettered immediately. A transient failure is dead-lettered after `max_attempts` claims.
- On shutdown, unfinished rows are released immediately and the interrupted attempt is not counted.

Because rows are retried out of order after a failure, ordering across retries is not guaranteed (see below). Run with `initialize_schema: true` or apply the migrations up to `000005`. Migration `000005` renames `quarantined_at` (introduced by `000004`) to `dead_lettered_at` and adds `failed_at` and `dead_letter_attempts`.

#### Without a dead-letter queue

A dead-lettered row stays in `outbox_messages` with `failed_at`, `dead_lettered_at`, `attempts` and `last_error` set, and is no longer claimed. Nothing is published and nothing alerts an operator except the log and metrics hooks below. Inspect them with:

```sql
SELECT id, channel, attempts, last_error FROM outbox_messages WHERE dead_lettered_at IS NOT NULL;
```

After fixing the cause (for example configuring the missing channel), requeue a row with:

```sql
UPDATE outbox_messages
SET dead_lettered_at = NULL, failed_at = NULL, attempts = 0, dead_letter_attempts = 0, next_attempt_at = NULL
WHERE id = '...';
```

#### Dead-letter queue (DLQ)

Set the optional top-level `dlq_channel` to the name of a configured channel to publish dead-lettered messages there instead of only parking them:

```yaml
dlq_channel: orders.dead-letter

channels:
  - name: orders.created
    publisher: { type: rabbitmq, data: { url: amqp://..., exchange: outbox.events, routing_key: orders.created } }
  - name: orders.dead-letter
    publisher: { type: kafka, data: { brokers: [kafka:9092], topic: outbox.dead-letter } }
```

The DLQ channel uses the normal publisher of that channel, so it may live on a different broker. Startup fails if `dlq_channel` does not name a configured channel. Messages whose own channel is the DLQ channel are retried like any other message, and if they dead-letter they have nowhere else to go: use a channel that outbox messages are not written to.

A dead-lettered message is published with the original `id`, payload, affinity key and metadata, plus these metadata entries (they override same-named keys):

| Key | Value |
| --- | --- |
| `x-outbox-original-channel` | Channel the message was written to. |
| `x-outbox-message-id` | Original message ID (also the message `ID`). |
| `x-outbox-attempts` | Number of publish attempts. |
| `x-outbox-last-error` | Last failure reason (truncated to 2000 bytes). |
| `x-outbox-failed-at` | RFC 3339 time the message was dead-lettered. |

How a row moves through the DLQ:

1. The failure is recorded (`failed_at`, `last_error`) while the row stays claimed.
2. The message is published to the DLQ channel.
3. Only after the DLQ publisher acknowledges it, `dead_lettered_at` is set and the row leaves the claim queue.
4. If the DLQ publish fails, the row is released and retried with exponential backoff (`retry_backoff` up to `max_retry_backoff`, counted in `dead_letter_attempts`) without any attempt limit. Rows with `failed_at` set never go back to their original channel and never block other rows.
5. If the forwarder crashes at any point, the row is claimed again after the lease expires and resumes at step 2.

Delivery to the DLQ is at-least-once: a crash after the DLQ acknowledged a message but before step 3 publishes it again. Deduplicate on `x-outbox-message-id`.

Rows remain in `outbox_messages` after they were dead-lettered (`dead_lettered_at IS NOT NULL`). Delete them once you no longer need them. Rows parked before `dlq_channel` was configured stay parked; replay them with the SQL above or delete them.

#### Replaying messages from the DLQ

Replay is performed by whoever owns the DLQ, after fixing the cause:

1. Consume the message from the DLQ.
2. Re-insert it into the outbox with the publisher library, using the original channel and a new or the original ID. Take the channel from `x-outbox-original-channel`, and drop the `x-outbox-*` metadata entries. Reusing the original ID keeps consumer-side deduplication working; use a new ID only if the consumer already recorded the original as processed and the replay must be handled again.
3. Acknowledge the DLQ message.

```go
msg := outbox.NewMessage(
	dlq.Metadata["x-outbox-message-id"],
	outbox.Channel(dlq.Metadata["x-outbox-original-channel"]),
	dlq.AffinityKey, dlq.Payload, withoutOutboxMetadata(dlq.Metadata),
)
err := publisher.Publish(ctx, msg)
```

A replayed message reaches the original channel at-least-once and may arrive out of order relative to newer messages. Messages that fail again are dead-lettered again.

#### Metrics and alerting

The forwarder logs `outbox message dead-lettered` at error level for every message and reports these metrics through the `Metrics` interface of the PostgreSQL source (default: structured `outbox metric` log lines, wire your own implementation for Prometheus or OpenTelemetry when embedding the source):

| Metric | Meaning |
| --- | --- |
| `outbox_dead_lettered_total` | Messages acknowledged by the DLQ, or parked when no DLQ is configured. |
| `outbox_dead_letter_retries_total` | Messages whose DLQ publish failed and was scheduled for retry. |
| `oldest_claimable_age_seconds` | Age of the oldest row that could be claimed now, sampled every 15s. Alert when it keeps growing. |
| `dead_lettered`, `pending_dead_letter` | Rows currently dead-lettered, and failed rows still waiting for the DLQ. Alert when `pending_dead_letter` does not drain. |

### Ordering

Within a single PostgreSQL claim batch, messages are passed to a publisher in ascending `occurred_at`, then `id`, order. This is a deterministic batch order only: the forwarder does not serialize an `AffinityKey` across claim batches or multiple forwarder instances. Two workers can publish same-key messages concurrently, and a later message can therefore arrive first. Systems that require strict per-key delivery order must enforce sequencing or serialization at the destination or in the application.

## RabbitMQ Publisher

```yaml
channels:
  - name: orders.created
    publisher:
      type: rabbitmq
      data:
        url: amqp://guest:guest@rabbitmq:5672/
        exchange: outbox.events
        routing_key: orders.created
        content_type: application/json
        delivery_mode: 2
        immediate: false
```

Fields:

| Field | Required | Description |
| --- | --- | --- |
| `url` | yes | RabbitMQ AMQP URL. |
| `exchange` | no | Exchange name. Empty string publishes to the default exchange. |
| `routing_key` | yes | Routing key used for publish. |
| `content_type` | no | Message content type. Default is `application/octet-stream`. |
| `delivery_mode` | no | Must be `2` (persistent); this is also the default. |
| `mandatory` | no | Deprecated and ignored. Publishing is always mandatory so unroutable messages are returned and the outbox row is retained. |
| `immediate` | no | AMQP immediate flag. |

The publisher enables RabbitMQ publisher confirms and waits for a broker ACK for
each message. It uses mandatory routing and persistent delivery. A broker NACK,
connection/channel failure, timeout, or returned (unroutable) publication fails
the batch, so the PostgreSQL source retains the claimed rows. Configure durable
exchanges and queues/bindings where the broker must preserve messages across a
restart. Delivery remains at-least-once: a process failure after a broker ACK and
before the source deletes the row can cause a duplicate publication.

### RabbitMQ Consumer Schema

RabbitMQ messages are published as AMQP properties plus the original payload body:

| AMQP field | Outbox field |
| --- | --- |
| `Body` | `Message.Payload` |
| `MessageId` | `Message.ID` |
| `CorrelationId` | `Message.ID` |
| `Type` | `Message.Channel` |
| `Timestamp` | `Message.OccurredAt` |
| `Headers` | `Message.Metadata` |
| `Headers["affinity_key"]` | `Message.AffinityKey` |

Consumers can use the public AMQP helpers to decode deliveries:

```go
import (
	outboxamqp "github.com/petretiandrea/outbox-go/pkg/outbox/amqp"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handleDelivery(delivery amqp.Delivery) error {
	msg, err := outboxamqp.MessageFromDelivery(delivery)
	if err != nil {
		return err
	}

	// msg.Payload is the application event body.
	// msg.Metadata contains string metadata headers.
	return process(msg)
}
```

## Kafka Publisher

```yaml
channels:
  - name: payments.completed
    publisher:
      type: kafka
      data:
        brokers:
          - kafka:9092
        topic: payments.events
        client_id: outbox-forwarder
        batch_bytes: 1048576
        batch_size: 100
        batch_timeout: 1s
        async: false
```

Fields:

| Field | Required | Description |
| --- | --- | --- |
| `brokers` | yes | List of Kafka brokers. |
| `topic` | yes | Kafka topic. |
| `client_id` | no | Kafka client id. |
| `batch_bytes` | no | Kafka writer batch byte limit. |
| `batch_size` | no | Kafka writer batch size. |
| `batch_timeout` | no | Kafka writer batch timeout, for example `1s`. |
| `async` | no | If `true`, Kafka writer publishes asynchronously. |

## Complete YAML Example

```yaml
source:
  type: postgres
  data:
    dsn: postgres://outbox:outbox@postgres:5432/outbox?sslmode=disable
    table_name: outbox_messages
    batch_size: 100
    poll_interval: 1s
    claim_lease: 30s
    initialize_schema: true

channels:
  - name: orders.created
    publisher:
      type: rabbitmq
      data:
        url: amqp://guest:guest@rabbitmq:5672/
        exchange: outbox.events
        routing_key: orders.created
        content_type: application/json

  - name: payments.completed
    publisher:
      type: kafka
      data:
        brokers:
          - kafka:9092
        topic: payments.events
        client_id: outbox-forwarder
```

## Environment Variables

The service also supports configuration through environment variables.

Rules:

- Default prefix is `OUTBOX_`.
- Use double underscores `__` for nested paths.
- Environment variables override values loaded from the YAML file.
- YAML values do not expand `${VARIABLE}` placeholders.

PostgreSQL source via env:

```bash
OUTBOX_SOURCE__TYPE=postgres
OUTBOX_SOURCE__DATA__DSN=postgres://outbox:outbox@postgres:5432/outbox?sslmode=disable
OUTBOX_SOURCE__DATA__TABLE_NAME=outbox_messages
OUTBOX_SOURCE__DATA__BATCH_SIZE=100
OUTBOX_SOURCE__DATA__POLL_INTERVAL=1s
OUTBOX_SOURCE__DATA__CLAIM_LEASE=30s
OUTBOX_SOURCE__DATA__INITIALIZE_SCHEMA=true
```

RabbitMQ channel via env:

```bash
OUTBOX_CHANNELS__0__NAME=orders.created
OUTBOX_CHANNELS__0__PUBLISHER__TYPE=rabbitmq
OUTBOX_CHANNELS__0__PUBLISHER__DATA__URL=amqp://guest:guest@rabbitmq:5672/
OUTBOX_CHANNELS__0__PUBLISHER__DATA__EXCHANGE=outbox.events
OUTBOX_CHANNELS__0__PUBLISHER__DATA__ROUTING_KEY=orders.created
OUTBOX_CHANNELS__0__PUBLISHER__DATA__CONTENT_TYPE=application/json
```

Kafka channel via env:

```bash
OUTBOX_CHANNELS__1__NAME=payments.completed
OUTBOX_CHANNELS__1__PUBLISHER__TYPE=kafka
OUTBOX_CHANNELS__1__PUBLISHER__DATA__BROKERS__0=kafka:9092
OUTBOX_CHANNELS__1__PUBLISHER__DATA__TOPIC=payments.events
OUTBOX_CHANNELS__1__PUBLISHER__DATA__CLIENT_ID=outbox-forwarder
```

For humans, the recommended production pattern is:

- YAML ConfigMap for non-secret structure.
- Environment variables or Secrets for DSN, passwords, tokens, and broker URLs.

## Running Locally

Run with a YAML config:

```bash
go run ./cmd run --config outbox.example.yaml
```

Run with env only:

```bash
OUTBOX_SOURCE__TYPE=postgres \
OUTBOX_SOURCE__DATA__DSN='postgres://outbox:outbox@localhost:5432/outbox?sslmode=disable' \
OUTBOX_SOURCE__DATA__INITIALIZE_SCHEMA=true \
OUTBOX_SOURCE__DATA__CLAIM_LEASE=30s \
OUTBOX_CHANNELS__0__NAME=orders.created \
OUTBOX_CHANNELS__0__PUBLISHER__TYPE=rabbitmq \
OUTBOX_CHANNELS__0__PUBLISHER__DATA__URL='amqp://guest:guest@localhost:5672/' \
OUTBOX_CHANNELS__0__PUBLISHER__DATA__ROUTING_KEY='orders.created' \
go run ./cmd run
```

On Windows PowerShell:

```powershell
$env:OUTBOX_SOURCE__TYPE = "postgres"
$env:OUTBOX_SOURCE__DATA__DSN = "postgres://outbox:outbox@localhost:5432/outbox?sslmode=disable"
$env:OUTBOX_SOURCE__DATA__INITIALIZE_SCHEMA = "true"
$env:OUTBOX_SOURCE__DATA__CLAIM_LEASE = "30s"
$env:OUTBOX_CHANNELS__0__NAME = "orders.created"
$env:OUTBOX_CHANNELS__0__PUBLISHER__TYPE = "rabbitmq"
$env:OUTBOX_CHANNELS__0__PUBLISHER__DATA__URL = "amqp://guest:guest@localhost:5672/"
$env:OUTBOX_CHANNELS__0__PUBLISHER__DATA__ROUTING_KEY = "orders.created"
go run ./cmd run
```

## Running With Docker

The image default command is:

```text
outbox run --config /etc/outbox/outbox.yaml
```

If the config file is mounted at `/etc/outbox/outbox.yaml`, no args are needed.

```bash
docker run --rm \
  -v "$PWD/outbox.example.yaml:/etc/outbox/outbox.yaml:ro" \
  your-dockerhub-user/outbox-forwarder:latest
```

Override only the DSN with an environment variable:

```bash
docker run --rm \
  -v "$PWD/outbox.example.yaml:/etc/outbox/outbox.yaml:ro" \
  -e OUTBOX_SOURCE__DATA__DSN='postgres://outbox:outbox@postgres:5432/outbox?sslmode=disable' \
  your-dockerhub-user/outbox-forwarder:latest
```

## Kubernetes Deployment Example

ConfigMap:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: outbox-forwarder-config
data:
  outbox.yaml: |
    source:
      type: postgres
      data:
        table_name: outbox_messages
        batch_size: 100
        poll_interval: 1s
        claim_lease: 30s
        initialize_schema: true

    channels:
      - name: orders.created
        publisher:
          type: rabbitmq
          data:
            exchange: outbox.events
            routing_key: orders.created
            content_type: application/json
```

Secret:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: outbox-forwarder-secrets
type: Opaque
stringData:
  postgres-dsn: postgres://outbox:outbox@postgres:5432/outbox?sslmode=disable
  rabbitmq-url: amqp://guest:guest@rabbitmq:5672/
```

Deployment:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: outbox-forwarder
spec:
  replicas: 1
  selector:
    matchLabels:
      app: outbox-forwarder
  template:
    metadata:
      labels:
        app: outbox-forwarder
    spec:
      containers:
        - name: outbox-forwarder
          image: your-dockerhub-user/outbox-forwarder:latest
          env:
            - name: OUTBOX_SOURCE__DATA__DSN
              valueFrom:
                secretKeyRef:
                  name: outbox-forwarder-secrets
                  key: postgres-dsn
            - name: OUTBOX_CHANNELS__0__PUBLISHER__DATA__URL
              valueFrom:
                secretKeyRef:
                  name: outbox-forwarder-secrets
                  key: rabbitmq-url
          volumeMounts:
            - name: config
              mountPath: /etc/outbox
              readOnly: true
      volumes:
        - name: config
          configMap:
            name: outbox-forwarder-config
```

No `args` are required in this Deployment because the Docker image already defaults to:

```text
run --config /etc/outbox/outbox.yaml
```

Only add `args` if you mount the config somewhere else.

## Operational Notes

- Start with `replicas: 1`.
- The forwarder uses at-least-once delivery. Consumers should be idempotent by `Message.ID`.
- Multiple replicas can share the same table because rows are claimed with a lease before publishing.
- `AffinityKey` is not a cross-replica ordering lock; it identifies a destination partition or shard only.
- Choose a `claim_lease` that tolerates transient database outages. Active handlers renew their lease, but if renewal fails the claim can be reclaimed after the last successful lease; a slow handler may then overlap with the new worker. Completion from the stale claim is fenced, but a publish already in flight cannot be undone.
- `initialize_schema: true` is useful for development and simple deployments.
- In stricter production environments, you may prefer to run migrations outside the service and set `initialize_schema: false`.
- The forwarder fails startup if the source or publishers cannot be created.
- The service does not expand `${ENV_VAR}` inside YAML values.
- Use env overrides for secrets.

## Troubleshooting

`postgres source dsn is required`

The service did not receive `source.data.dsn`. Add it to YAML or set `OUTBOX_SOURCE__DATA__DSN`.

`unsupported source type`

Set `source.type: postgres`.

`unsupported publisher type`

Publisher type must be `rabbitmq`, `rabbit`, or `kafka`.

`postgres source only supports table_name "outbox_messages"`

The generated PostgreSQL queries target the default table. Use `table_name: outbox_messages`.

No messages are published.

Check that the message `channel` stored in PostgreSQL exactly matches one of the configured `channels[].name` values.

RabbitMQ publish fails.

Check `url`, `exchange`, and `routing_key`. If publishing to the default exchange, leave `exchange` empty and set `routing_key` to the queue name.

Kafka publish fails.

Check that at least one broker is reachable and that `topic` is set.
