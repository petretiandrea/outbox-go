package kafka

import (
	"errors"
	"time"

	"petretiandrea.github.com/outbox/internal/config"
	"petretiandrea.github.com/outbox/pkg/outbox"
)

type FactoryConfig struct {
	Brokers      []string `koanf:"brokers" yaml:"brokers" json:"brokers"`
	Topic        string   `koanf:"topic" yaml:"topic" json:"topic"`
	ClientID     string   `koanf:"client_id" yaml:"client_id" json:"client_id"`
	BatchBytes   int      `koanf:"batch_bytes" yaml:"batch_bytes" json:"batch_bytes"`
	BatchSize    int      `koanf:"batch_size" yaml:"batch_size" json:"batch_size"`
	BatchTimeout string   `koanf:"batch_timeout" yaml:"batch_timeout" json:"batch_timeout"`
	Async        bool     `koanf:"async" yaml:"async" json:"async"`
}

func BuildPublisherFromConfig(publisherConfig config.PublisherConfig) (outbox.Publisher, error) {
	if len(publisherConfig.Data) == 0 {
		return nil, errors.New("kafka publisher data is required")
	}

	var factoryConfig FactoryConfig
	if err := config.DecodeMap(publisherConfig.Data, &factoryConfig); err != nil {
		return nil, err
	}

	publisherConfigData, err := factoryConfig.toPublisherConfig()
	if err != nil {
		return nil, err
	}

	return NewPublisher(publisherConfigData)
}

func (c FactoryConfig) toPublisherConfig() (PublisherConfig, error) {
	batchTimeout := time.Duration(0)
	if c.BatchTimeout != "" {
		parsed, err := time.ParseDuration(c.BatchTimeout)
		if err != nil {
			return PublisherConfig{}, err
		}
		batchTimeout = parsed
	}

	return PublisherConfig{
		Brokers:      c.Brokers,
		Topic:        c.Topic,
		ClientID:     c.ClientID,
		BatchBytes:   c.BatchBytes,
		BatchSize:    c.BatchSize,
		BatchTimeout: batchTimeout,
		Async:        c.Async,
	}, nil
}
