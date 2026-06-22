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
| `table_name` | no | Outbox table name. Default is `outbox_messages`. |
| `batch_size` | no | Max messages fetched per poll. Default is `100`. |
| `poll_interval` | no | Delay when no messages are found. Default is `1s`. |
| `claim_lease` | no | How long a forwarder owns a claimed batch before another forwarder can retry it. Default is `30s`. |
| `claim_owner` | no | Identifier stored in claimed rows. Default is generated from hostname and process id. |
| `initialize_schema` | no | If `true`, runs embedded PostgreSQL migrations during startup before subscribing. Default is `false`. |

`initialize_schema` is executed before the first subscribe loop. If schema initialization fails, the service fails fast and does not start processing.

Current schema initialization supports the default table name `outbox_messages`.

The PostgreSQL source claims rows before publishing and deletes them only after the publish succeeds. If publishing fails, the claim is released and the row can be retried. If the process crashes after publishing but before delete, the message can be published again after the claim lease expires, so consumers should treat delivery as at-least-once and use `Message.ID` for idempotency.

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
        mandatory: false
        immediate: false
```

Fields:

| Field | Required | Description |
| --- | --- | --- |
| `url` | yes | RabbitMQ AMQP URL. |
| `exchange` | no | Exchange name. Empty string publishes to the default exchange. |
| `routing_key` | yes | Routing key used for publish. |
| `content_type` | no | Message content type. Default is `application/octet-stream`. |
| `delivery_mode` | no | AMQP delivery mode. Default is `2` for persistent. |
| `mandatory` | no | AMQP mandatory flag. |
| `immediate` | no | AMQP immediate flag. |

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
- Set `claim_lease` longer than the expected maximum publish time. If the process crashes, another replica can retry after the lease expires.
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

`initialize_schema only supports table_name "outbox_messages"`

Embedded migrations currently create the default table. Use `table_name: outbox_messages` or disable `initialize_schema` and manage custom schema yourself. Custom tables must include the claim columns `claimed_by`, `claimed_until`, `attempts`, and `last_error`.

No messages are published.

Check that the message `channel` stored in PostgreSQL exactly matches one of the configured `channels[].name` values.

RabbitMQ publish fails.

Check `url`, `exchange`, and `routing_key`. If publishing to the default exchange, leave `exchange` empty and set `routing_key` to the queue name.

Kafka publish fails.

Check that at least one broker is reachable and that `topic` is set.
