# syntax=docker/dockerfile:1

FROM golang:1.25-alpine AS build

WORKDIR /src

RUN apk add --no-cache make

COPY go.mod go.sum ./
COPY pkg/outbox/go.mod pkg/outbox/go.sum ./pkg/outbox/
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux make SHELL=/bin/sh build-linux

FROM alpine:3.22

RUN addgroup -S outbox && adduser -S -G outbox outbox

COPY --from=build /src/bin/outbox /usr/local/bin/outbox

USER outbox

ENTRYPOINT ["outbox"]
CMD ["run", "--config", "/etc/outbox/outbox.yaml"]
