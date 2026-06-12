package rabbitmq

import (
	"errors"

	"petretiandrea.github.com/outbox/internal/config"
	"petretiandrea.github.com/outbox/pkg/outbox"
)

type FactoryConfig struct {
	URL          string `koanf:"url" yaml:"url" json:"url"`
	Exchange     string `koanf:"exchange" yaml:"exchange" json:"exchange"`
	RoutingKey   string `koanf:"routing_key" yaml:"routing_key" json:"routing_key"`
	ContentType  string `koanf:"content_type" yaml:"content_type" json:"content_type"`
	DeliveryMode uint8  `koanf:"delivery_mode" yaml:"delivery_mode" json:"delivery_mode"`
	Mandatory    bool   `koanf:"mandatory" yaml:"mandatory" json:"mandatory"`
	Immediate    bool   `koanf:"immediate" yaml:"immediate" json:"immediate"`
}

func BuildPublisherFromConfig(publisherConfig config.PublisherConfig) (outbox.Publisher, error) {
	if len(publisherConfig.Data) == 0 {
		return nil, errors.New("rabbitmq publisher data is required")
	}

	var factoryConfig FactoryConfig
	if err := config.DecodeMap(publisherConfig.Data, &factoryConfig); err != nil {
		return nil, err
	}

	return NewPublisher(PublisherConfig{
		URL:          factoryConfig.URL,
		Exchange:     factoryConfig.Exchange,
		RoutingKey:   factoryConfig.RoutingKey,
		ContentType:  factoryConfig.ContentType,
		DeliveryMode: factoryConfig.DeliveryMode,
		Mandatory:    factoryConfig.Mandatory,
		Immediate:    factoryConfig.Immediate,
	})
}
