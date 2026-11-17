# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS build

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

RUN apk add --no-cache make

COPY go.mod go.sum ./
COPY pkg/outbox/go.mod pkg/outbox/go.sum ./pkg/outbox/
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} make SHELL=/bin/sh build-linux

FROM alpine:3.22

RUN addgroup -S outbox && adduser -S -G outbox outbox

COPY --from=build /src/bin/outbox /usr/local/bin/outbox

USER outbox

ENTRYPOINT ["outbox"]
CMD ["run", "--config", "/etc/outbox/outbox.yaml"]
