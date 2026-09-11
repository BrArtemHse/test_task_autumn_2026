package migrations

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

const (
	firstMigrationSQL  = "CREATE SCHEMA IF NOT EXISTS orders;"
	secondMigrationSQL = "CREATE TABLE orders.orders (id TEXT PRIMARY KEY);"
)

func TestRunLocksSchemaMigrationsBootstrap(t *testing.T) {
	db, mock := newMockDB(t)
	dir := t.TempDir()

	expectMigrationsBootstrap(mock)

	err := Run(context.Background(), db, Config{
		Service:   "order-service",
		Directory: dir,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestRunAppliesPendingMigrationsInFilenameOrder(t *testing.T) {
	db, mock := newMockDB(t)
	dir := writeMigrations(t, map[string]string{
		"0002_orders.sql": secondMigrationSQL,
		"0001_schema.sql": firstMigrationSQL,
	})

	expectMigrationsBootstrap(mock)
	expectPendingMigration(mock, "order-service", "0001", firstMigrationSQL)
	expectPendingMigration(mock, "order-service", "0002", secondMigrationSQL)

	err := Run(context.Background(), db, Config{
		Service:   "order-service",
		Directory: dir,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestRunSkipsAppliedMigrationWithSameChecksum(t *testing.T) {
	db, mock := newMockDB(t)
	dir := writeMigrations(t, map[string]string{
		"0001_schema.sql": firstMigrationSQL,
	})

	expectMigrationsBootstrap(mock)
	mock.ExpectBegin()
	expectMigrationLock(mock, "order-service")
	mock.ExpectQuery(selectAppliedMigrationSQL).
		WithArgs("order-service", "0001").
		WillReturnRows(sqlmock.NewRows([]string{"checksum"}).AddRow(checksum(firstMigrationSQL)))
	mock.ExpectCommit()

	err := Run(context.Background(), db, Config{
		Service:   "order-service",
		Directory: dir,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestRunRejectsAppliedMigrationWithDifferentChecksum(t *testing.T) {
	db, mock := newMockDB(t)
	dir := writeMigrations(t, map[string]string{
		"0001_schema.sql": firstMigrationSQL,
	})

	expectMigrationsBootstrap(mock)
	mock.ExpectBegin()
	expectMigrationLock(mock, "order-service")
	mock.ExpectQuery(selectAppliedMigrationSQL).
		WithArgs("order-service", "0001").
		WillReturnRows(sqlmock.NewRows([]string{"checksum"}).AddRow("old-checksum"))
	mock.ExpectRollback()

	err := Run(context.Background(), db, Config{
		Service:   "order-service",
		Directory: dir,
	})
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("Run() error = %v, want %v", err, ErrChecksumMismatch)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestRunRejectsInvalidConfig(t *testing.T) {
	err := Run(context.Background(), nil, Config{
		Service:   "order-service",
		Directory: "migrations/order",
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Run() error = %v, want %v", err, ErrInvalidConfig)
	}
}

func newMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return db, mock
}

func writeMigrations(t *testing.T, files map[string]string) string {
	t.Helper()

	dir := t.TempDir()
	for name, contents := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", name, err)
		}
	}
	return dir
}

func expectPendingMigration(mock sqlmock.Sqlmock, service, version, contents string) {
	mock.ExpectBegin()
	expectMigrationLock(mock, service)
	mock.ExpectQuery(selectAppliedMigrationSQL).
		WithArgs(service, version).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(contents).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(insertAppliedMigrationSQL).
		WithArgs(service, version, sqlmock.AnyArg(), checksum(contents)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
}

func expectMigrationLock(mock sqlmock.Sqlmock, service string) {
	mock.ExpectExec(migrationLockSQL).
		WithArgs("migrations:" + service).
		WillReturnResult(sqlmock.NewResult(0, 0))
}

func expectMigrationsBootstrap(mock sqlmock.Sqlmock) {
	mock.ExpectBegin()
	mock.ExpectExec(migrationLockSQL).
		WithArgs(migrationsBootstrapLockName).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(ensureSchemaMigrationsSQL).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
}

func checksum(contents string) string {
	sum := sha256.Sum256([]byte(contents))
	return hex.EncodeToString(sum[:])
}
