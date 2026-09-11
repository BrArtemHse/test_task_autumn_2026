package migrations

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var (
	ErrInvalidConfig    = errors.New("invalid migration config")
	ErrChecksumMismatch = errors.New("migration checksum mismatch")
)

const (
	migrationsBootstrapLockName = "migrations:bootstrap"
	ensureSchemaMigrationsSQL   = `CREATE TABLE IF NOT EXISTS schema_migrations (
    service TEXT NOT NULL,
    version TEXT NOT NULL,
    filename TEXT NOT NULL,
    checksum TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (service, version)
)`
	migrationLockSQL          = "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"
	selectAppliedMigrationSQL = "SELECT checksum FROM schema_migrations WHERE service = $1 AND version = $2"
	insertAppliedMigrationSQL = "INSERT INTO schema_migrations (service, version, filename, checksum) VALUES ($1, $2, $3, $4)"
)

type Config struct {
	Service   string
	Directory string
}

type migration struct {
	version  string
	filename string
	sql      string
	checksum string
}

func Run(ctx context.Context, db *sql.DB, config Config) error {
	if db == nil || strings.TrimSpace(config.Service) == "" || strings.TrimSpace(config.Directory) == "" {
		return ErrInvalidConfig
	}

	migrations, err := loadMigrations(config.Directory)
	if err != nil {
		return err
	}

	if err := ensureMigrationsTable(ctx, db); err != nil {
		return err
	}

	for _, item := range migrations {
		if err := applyMigration(ctx, db, config.Service, item); err != nil {
			return err
		}
	}

	return nil
}

func ensureMigrationsTable(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	committed := false
	defer rollbackUnlessCommitted(tx, &committed)

	if _, err := tx.ExecContext(ctx, migrationLockSQL, migrationsBootstrapLockName); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, ensureSchemaMigrationsSQL); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true

	return nil
}

func loadMigrations(directory string) ([]migration, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}

	items := make([]migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}

		path := filepath.Join(directory, entry.Name())
		contents, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}

		sqlText := strings.TrimSpace(string(contents))
		if sqlText == "" {
			return nil, fmt.Errorf("%w: empty migration %s", ErrInvalidConfig, entry.Name())
		}

		items = append(items, migration{
			version:  migrationVersion(entry.Name()),
			filename: entry.Name(),
			sql:      sqlText,
			checksum: migrationChecksum(sqlText),
		})
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].filename < items[j].filename
	})

	return items, nil
}

func migrationVersion(filename string) string {
	base := strings.TrimSuffix(filename, filepath.Ext(filename))
	version, _, found := strings.Cut(base, "_")
	if found {
		return version
	}
	return base
}

func migrationChecksum(contents string) string {
	sum := sha256.Sum256([]byte(contents))
	return hex.EncodeToString(sum[:])
}

func applyMigration(ctx context.Context, db *sql.DB, service string, item migration) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	committed := false
	defer rollbackUnlessCommitted(tx, &committed)

	if _, err := tx.ExecContext(ctx, migrationLockSQL, "migrations:"+service); err != nil {
		return err
	}

	var storedChecksum string
	err = tx.QueryRowContext(ctx, selectAppliedMigrationSQL, service, item.version).Scan(&storedChecksum)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return err
	case storedChecksum == item.checksum:
		if err := tx.Commit(); err != nil {
			return err
		}
		committed = true
		return nil
	default:
		return fmt.Errorf("%w: %s/%s", ErrChecksumMismatch, service, item.filename)
	}

	if _, err := tx.ExecContext(ctx, item.sql); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, insertAppliedMigrationSQL, service, item.version, item.filename, item.checksum); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true

	return nil
}

func rollbackUnlessCommitted(tx *sql.Tx, committed *bool) {
	if !*committed {
		_ = tx.Rollback()
	}
}
