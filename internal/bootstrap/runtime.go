package bootstrap

import (
	"context"
	"errors"

	"petretiandrea.github.com/outbox/internal/config"
	"petretiandrea.github.com/outbox/internal/domain"
)

type Runtime struct {
	processor *domain.OutboxProcessor
}

func NewRuntime(config config.ServiceConfig) (*Runtime, error) {
	return NewRuntimeWithFactories(config, NewDefaultSourceFactory(), NewDefaultPublisherFactory())
}

func NewRuntimeWithFactories(config config.ServiceConfig, sourceFactory *SourceFactory, publisherFactory *PublisherFactory) (*Runtime, error) {
	source, err := sourceFactory.Build(config.Source)
	if err != nil {
		return nil, err
	}

	publishers, err := publisherFactory.BuildRegistry(config.Channels)
	if err != nil {
		_ = source.Close()
		return nil, err
	}

	processor, err := domain.NewOutboxProcessor(domain.OutboxProcessorConfig{
		Source:     source,
		Publishers: publishers,
	})
	if err != nil {
		for _, publisher := range publishers {
			_ = publisher.Close()
		}
		_ = source.Close()
		return nil, err
	}

	return &Runtime{
		processor: processor,
	}, nil
}

func (r *Runtime) Run(ctx context.Context) error {
	if r == nil || r.processor == nil {
		return errors.New("service runtime is not initialized")
	}

	err := r.processor.Process(ctx)
	if err != nil && errors.Is(err, context.Canceled) {
		return nil
	}

	return err
}

func (r *Runtime) Close() error {
	if r == nil || r.processor == nil {
		return nil
	}

	return r.processor.Close()
}
