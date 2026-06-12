SHELL := powershell.exe
.SHELLFLAGS := -NoProfile -Command

APP_NAME := outbox
BIN_DIR := bin
MAIN_PACKAGE := ./cmd
CONFIG ?= outbox.example.yaml
DSN ?= postgres://outbox:outbox@localhost:5432/outbox?sslmode=disable
OUTBOX_TABLE ?= outbox_messages
OUTBOX_CHANNEL ?= benchmark
OUTBOX_COUNT ?= 10000
OUTBOX_BATCH_SIZE ?= 100
OUTBOX_PAYLOAD_SIZE ?= 256
OUTBOX_POLL_INTERVAL ?= 10ms

.PHONY: build test run benchmark-forwarder up down logs clean

build:
	if (!(Test-Path $(BIN_DIR))) { New-Item -ItemType Directory -Path $(BIN_DIR) | Out-Null }
	go build -o $(BIN_DIR)/$(APP_NAME) $(MAIN_PACKAGE)

test:
	go test ./...

run:
	go run $(MAIN_PACKAGE) run --config $(CONFIG)

benchmark-forwarder:
	go run $(MAIN_PACKAGE) benchmark postgres --dsn "$(DSN)" --table $(OUTBOX_TABLE) --channel $(OUTBOX_CHANNEL) --count $(OUTBOX_COUNT) --batch-size $(OUTBOX_BATCH_SIZE) --payload-size $(OUTBOX_PAYLOAD_SIZE) --truncate-first --measure-forwarder --poll-interval $(OUTBOX_POLL_INTERVAL)

clean:
	if (Test-Path $(BIN_DIR)) { Remove-Item -Recurse -Force $(BIN_DIR) }
