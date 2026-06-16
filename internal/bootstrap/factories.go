package bootstrap

import (
	"errors"
	"fmt"
	"strings"

	"github.com/petretiandrea/outbox-go/internal/config"
	"github.com/petretiandrea/outbox-go/internal/domain"
	"github.com/petretiandrea/outbox-go/internal/infrastructure/kafka"
	"github.com/petretiandrea/outbox-go/internal/infrastructure/postgres"
	"github.com/petretiandrea/outbox-go/internal/infrastructure/rabbitmq"
	"github.com/petretiandrea/outbox-go/pkg/outbox"
)

type SourceBuilder func(config.SourceConfig) (domain.Source, error)

type SourceFactory struct {
	builders map[string]SourceBuilder
}

func NewSourceFactory() *SourceFactory {
	return &SourceFactory{
		builders: make(map[string]SourceBuilder),
	}
}

func NewDefaultSourceFactory() *SourceFactory {
	factory := NewSourceFactory()
	factory.Register("postgres", postgres.BuildSourceFromConfig)
	return factory
}

func (f *SourceFactory) Register(sourceType string, builder SourceBuilder) {
	if f == nil {
		return
	}

	normalizedType := normalizeType(sourceType)
	if normalizedType == "" || builder == nil {
		return
	}

	f.builders[normalizedType] = builder
}

func (f *SourceFactory) Build(config config.SourceConfig) (domain.Source, error) {
	if f == nil {
		return nil, errors.New("source factory is required")
	}

	sourceType := normalizeType(config.Type)
	if sourceType == "" {
		return nil, errors.New("source type is required")
	}

	builder, ok := f.builders[sourceType]
	if !ok {
		return nil, fmt.Errorf("unsupported source type %q", config.Type)
	}

	return builder(config)
}

type PublisherBuilder func(config.PublisherConfig) (outbox.Publisher, error)

type PublisherFactory struct {
	builders map[string]PublisherBuilder
}

func NewPublisherFactory() *PublisherFactory {
	return &PublisherFactory{
		builders: make(map[string]PublisherBuilder),
	}
}

func NewDefaultPublisherFactory() *PublisherFactory {
	factory := NewPublisherFactory()
	factory.Register("kafka", kafka.BuildPublisherFromConfig)
	factory.Register("rabbitmq", rabbitmq.BuildPublisherFromConfig)
	factory.Register("rabbit", rabbitmq.BuildPublisherFromConfig)
	return factory
}

func (f *PublisherFactory) Register(publisherType string, builder PublisherBuilder) {
	if f == nil {
		return
	}

	normalizedType := normalizeType(publisherType)
	if normalizedType == "" || builder == nil {
		return
	}

	f.builders[normalizedType] = builder
}

func (f *PublisherFactory) Build(config config.PublisherConfig) (outbox.Publisher, error) {
	if f == nil {
		return nil, errors.New("publisher factory is required")
	}

	publisherType := normalizeType(config.Type)
	if publisherType == "" {
		return nil, errors.New("publisher type is required")
	}

	builder, ok := f.builders[publisherType]
	if !ok {
		return nil, fmt.Errorf("unsupported publisher type %q", config.Type)
	}

	return builder(config)
}

func (f *PublisherFactory) BuildRegistry(channels []config.ChannelConfig) (map[outbox.Channel]outbox.Publisher, error) {
	if len(channels) == 0 {
		return nil, errors.New("at least one channel configuration is required")
	}

	registry := make(map[outbox.Channel]outbox.Publisher, len(channels))
	for _, channelConfig := range channels {
		channelName := strings.TrimSpace(channelConfig.Name)
		if channelName == "" {
			return nil, errors.New("channel name is required")
		}

		channel := outbox.Channel(channelName)
		if _, exists := registry[channel]; exists {
			return nil, fmt.Errorf("duplicate channel configuration for %q", channel)
		}

		publisher, err := f.Build(channelConfig.Publisher)
		if err != nil {
			return nil, fmt.Errorf("build publisher for channel %q: %w", channel, err)
		}

		registry[channel] = publisher
	}

	return registry, nil
}

func normalizeType(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
