package config

import (
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/v2"
)

const defaultEnvPrefix = "OUTBOX_"

var envPlaceholderPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

type LoadOptions struct {
	FilePath  string
	EnvPrefix string
}

func Load(options LoadOptions) (ServiceConfig, error) {
	k := koanf.New(".")

	if strings.TrimSpace(options.FilePath) != "" {
		configMap, err := loadFileConfig(options.FilePath)
		if err != nil {
			return ServiceConfig{}, fmt.Errorf("load config file: %w", err)
		}
		if err := k.Load(confmap.Provider(configMap, "."), nil); err != nil {
			return ServiceConfig{}, fmt.Errorf("load config map: %w", err)
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

func loadFileConfig(filePath string) (map[string]any, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	expanded, err := expandEnvPlaceholders(string(content))
	if err != nil {
		return nil, err
	}

	configMap, err := yaml.Parser().Unmarshal([]byte(expanded))
	if err != nil {
		return nil, err
	}

	return configMap, nil
}

func expandEnvPlaceholders(content string) (string, error) {
	var missing []string
	for _, match := range envPlaceholderPattern.FindAllStringSubmatch(content, -1) {
		name := match[1]
		if _, ok := os.LookupEnv(name); !ok && !slices.Contains(missing, name) {
			missing = append(missing, name)
		}
	}

	if len(missing) > 0 {
		return "", fmt.Errorf("missing environment variables referenced by config: %s", strings.Join(missing, ", "))
	}

	return envPlaceholderPattern.ReplaceAllStringFunc(content, func(placeholder string) string {
		name := envPlaceholderPattern.FindStringSubmatch(placeholder)[1]
		return os.Getenv(name)
	}), nil
}
