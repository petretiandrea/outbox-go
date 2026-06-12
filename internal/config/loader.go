package config

import (
	"fmt"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

const defaultEnvPrefix = "OUTBOX_"

type LoadOptions struct {
	FilePath  string
	EnvPrefix string
}

func Load(options LoadOptions) (ServiceConfig, error) {
	k := koanf.New(".")

	if strings.TrimSpace(options.FilePath) != "" {
		if err := k.Load(file.Provider(options.FilePath), yaml.Parser()); err != nil {
			return ServiceConfig{}, fmt.Errorf("load config file: %w", err)
		}
	}

	envPrefix := strings.TrimSpace(options.EnvPrefix)
	if envPrefix == "" {
		envPrefix = defaultEnvPrefix
	}

	if err := k.Load(env.Provider(envPrefix, ".", func(key string) string {
		trimmed := strings.TrimPrefix(key, envPrefix)
		trimmed = strings.ToLower(trimmed)
		return strings.ReplaceAll(trimmed, "__", ".")
	}), nil); err != nil {
		return ServiceConfig{}, fmt.Errorf("load env config: %w", err)
	}

	var config ServiceConfig
	if err := k.UnmarshalWithConf("", &config, koanf.UnmarshalConf{Tag: "koanf"}); err != nil {
		return ServiceConfig{}, fmt.Errorf("unmarshal config: %w", err)
	}

	return config, nil
}
