package config

import (
	"fmt"

	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/v2"
)

func DecodeMap(data map[string]any, target any) error {
	k := koanf.New(".")
	if err := k.Load(confmap.Provider(data, "."), nil); err != nil {
		return fmt.Errorf("load config map: %w", err)
	}

	if err := k.UnmarshalWithConf("", target, koanf.UnmarshalConf{Tag: "koanf"}); err != nil {
		return fmt.Errorf("unmarshal config map: %w", err)
	}

	return nil
}
