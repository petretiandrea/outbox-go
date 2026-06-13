package postgres

import (
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	migratepostgres "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

func initializeSchema(dsn string, tableName string) error {
	if normalizedTableName(tableName) != defaultTableName {
		return fmt.Errorf("initialize_schema only supports table_name %q", defaultTableName)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open postgres migration connection: %w", err)
	}
	defer db.Close()

	sourceDriver, err := iofs.New(migrationFiles, "migrations")
	if err != nil {
		return fmt.Errorf("create postgres migration source: %w", err)
	}
	defer sourceDriver.Close()

	databaseDriver, err := migratepostgres.WithInstance(db, &migratepostgres.Config{})
	if err != nil {
		return fmt.Errorf("create postgres migration database: %w", err)
	}
	defer databaseDriver.Close()

	migrator, err := migrate.NewWithInstance("iofs", sourceDriver, "postgres", databaseDriver)
	if err != nil {
		return fmt.Errorf("create postgres migrator: %w", err)
	}
	defer migrator.Close()

	if err := migrator.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("initialize postgres schema: %w", err)
	}

	return nil
}

func normalizedTableName(tableName string) string {
	normalized := strings.TrimSpace(tableName)
	if normalized == "" {
		return defaultTableName
	}
	return normalized
}
