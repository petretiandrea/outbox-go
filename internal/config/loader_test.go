package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadExpandsEnvPlaceholdersInFileConfig(t *testing.T) {
	t.Setenv("POSTGRES_DSN", "postgres://outbox:secret@postgres:5432/outbox?sslmode=disable")
	t.Setenv("RABBITMQ_URL", "amqp://secret:secret@rabbitmq:5672/")

	configPath := writeTestConfig(t, `source:
  type: postgres
  data:
    dsn: ${POSTGRES_DSN}
    table_name: outbox_messages

channels:
  - name: orders.created
    publisher:
      type: rabbitmq
      data:
        url: ${RABBITMQ_URL}
        exchange: outbox.events
        routing_key: orders.created
        content_type: application/json
`)

	config, err := Load(LoadOptions{
		FilePath:  configPath,
		EnvPrefix: "TEST_OUTBOX_",
	})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if config.Source.Data["dsn"] != "postgres://outbox:secret@postgres:5432/outbox?sslmode=disable" {
		t.Fatalf("expected postgres dsn from env, got %#v", config.Source.Data["dsn"])
	}

	if got := len(config.Channels); got != 1 {
		t.Fatalf("expected 1 channel, got %d", got)
	}

	channel := config.Channels[0]
	if channel.Name != "orders.created" {
		t.Fatalf("expected channel name from file, got %q", channel.Name)
	}
	if channel.Publisher.Type != "rabbitmq" {
		t.Fatalf("expected publisher type from file, got %q", channel.Publisher.Type)
	}

	data := channel.Publisher.Data
	if data["url"] != "amqp://secret:secret@rabbitmq:5672/" {
		t.Fatalf("expected publisher url from env, got %#v", data["url"])
	}
	if data["exchange"] != "outbox.events" {
		t.Fatalf("expected publisher exchange from file, got %#v", data["exchange"])
	}
}

func TestLoadFailsWhenEnvPlaceholderIsMissing(t *testing.T) {
	configPath := writeTestConfig(t, `source:
  type: postgres
  data:
    dsn: ${MISSING_POSTGRES_DSN}
`)

	_, err := Load(LoadOptions{
		FilePath:  configPath,
		EnvPrefix: "TEST_OUTBOX_",
	})
	if err == nil {
		t.Fatal("expected load error")
	}
	if !strings.Contains(err.Error(), "MISSING_POSTGRES_DSN") {
		t.Fatalf("expected missing env var in error, got %v", err)
	}
}

func writeTestConfig(t *testing.T, content string) string {
	t.Helper()

	configPath := filepath.Join(t.TempDir(), "outbox.yaml")
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	return configPath
}
