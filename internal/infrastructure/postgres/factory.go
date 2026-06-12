package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"petretiandrea.github.com/outbox/internal/config"
	"petretiandrea.github.com/outbox/internal/domain"
)

type FactoryConfig struct {
	DSN          string `koanf:"dsn" yaml:"dsn" json:"dsn"`
	TableName    string `koanf:"table_name" yaml:"table_name" json:"table_name"`
	BatchSize    int    `koanf:"batch_size" yaml:"batch_size" json:"batch_size"`
	PollInterval string `koanf:"poll_interval" yaml:"poll_interval" json:"poll_interval"`
}

func BuildSourceFromConfig(sourceConfig config.SourceConfig) (domain.Source, error) {
	if len(sourceConfig.Data) == 0 {
		return nil, errors.New("postgres source data is required")
	}

	var factoryConfig FactoryConfig
	if err := config.DecodeMap(sourceConfig.Data, &factoryConfig); err != nil {
		return nil, err
	}

	sourceConfigData, dsn, err := factoryConfig.toSourceConfig()
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	source, err := NewPollerSourceFromPool(pool, sourceConfigData)
	if err != nil {
		pool.Close()
		return nil, err
	}

	return source, nil
}

func (c FactoryConfig) toSourceConfig() (SourceConfig, string, error) {
	if c.DSN == "" {
		return SourceConfig{}, "", errors.New("postgres source dsn is required")
	}

	pollInterval := time.Duration(0)
	if c.PollInterval != "" {
		parsed, err := time.ParseDuration(c.PollInterval)
		if err != nil {
			return SourceConfig{}, "", err
		}
		pollInterval = parsed
	}

	return SourceConfig{
		TableName:    c.TableName,
		BatchSize:    c.BatchSize,
		PollInterval: pollInterval,
	}, c.DSN, nil
}
