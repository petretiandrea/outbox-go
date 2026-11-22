package config

type ServiceConfig struct {
	Source   SourceConfig    `koanf:"source" yaml:"source" json:"source"`
	Channels []ChannelConfig `koanf:"channels" yaml:"channels" json:"channels"`
}

type SourceConfig struct {
	Type string         `koanf:"type" yaml:"type" json:"type"`
	Data map[string]any `koanf:"data" yaml:"data" json:"data"`
}

type ChannelConfig struct {
	Name      string          `koanf:"name" yaml:"name" json:"name"`
	Publisher PublisherConfig `koanf:"publisher" yaml:"publisher" json:"publisher"`
}

type PublisherConfig struct {
	Type string         `koanf:"type" yaml:"type" json:"type"`
	Data map[string]any `koanf:"data" yaml:"data" json:"data"`
}
