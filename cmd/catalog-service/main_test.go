package main

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestNewCatalogStoreRunsMigrationsBeforeReturningStore(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "0001_schema.sql"), []byte("CREATE SCHEMA IF NOT EXISTS catalog;"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	mock.ExpectBegin()
	mock.ExpectExec("SELECT pg_advisory_xact_lock").
		WithArgs("migrations:bootstrap").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS schema_migrations").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec("SELECT pg_advisory_xact_lock").
		WithArgs("migrations:catalog-service").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT checksum FROM schema_migrations").
		WithArgs("catalog-service", "0001").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS catalog;").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO schema_migrations").
		WithArgs("catalog-service", "0001", "0001_schema.sql", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	store, err := newCatalogStore(context.Background(), db, dir)
	if err != nil {
		t.Fatalf("newCatalogStore() error = %v", err)
	}
	if store == nil {
		t.Fatal("store is nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}
