package main

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	partnerapp "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/application"
	partnerpostgres "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/postgres"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/kafkaconsume"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/kafkapub"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/runtime"
)

func TestNewPartnerStoreRunsMigrationsBeforeReturningStore(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "0001_schema.sql"), []byte("CREATE SCHEMA IF NOT EXISTS partner;"), 0o600); err != nil {
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
		WithArgs("migrations:partner-service").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT checksum FROM schema_migrations").
		WithArgs("partner-service", "0001").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS partner;").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO schema_migrations").
		WithArgs("partner-service", "0001", "0001_schema.sql", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	store, err := newPartnerStore(context.Background(), db, dir)
	if err != nil {
		t.Fatalf("newPartnerStore() error = %v", err)
	}
	if store == nil {
		t.Fatal("store is nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestNewOrderConsumerRejectsMissingKafkaBrokers(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	service := partnerapp.NewService(partnerapp.ServiceConfig{
		Store: partnerpostgres.NewStore(db),
		NewID: func() string { return "sub-1" },
	})

	_, err = newOrderConsumer(runtime.Config{
		OrderEventsTopic:         "orders.events.v1",
		OrderEventsConsumerGroup: "partner-service-orders-v1",
		KafkaConsumerRetryDelay:  time.Second,
	}, service)
	if !errors.Is(err, kafkaconsume.ErrInvalidConfig) {
		t.Fatalf("newOrderConsumer() error = %v, want %v", err, kafkaconsume.ErrInvalidConfig)
	}
}

func TestNewSubmissionDispatcherBuildsFromRuntimeConfig(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	dispatcher, err := newSubmissionDispatcher(db, runtime.Config{
		PartnerDispatchBatchSize:    20,
		PartnerDispatchPollInterval: time.Second,
		PartnerDispatchLockTTL:      30 * time.Second,
		PartnerDispatchRetryDelay:   5 * time.Second,
		PartnerDispatchMaxAttempts:  5,
		PartnerHTTPTimeout:          3 * time.Second,
	}, "dispatcher-1")
	if err != nil {
		t.Fatalf("newSubmissionDispatcher() error = %v", err)
	}
	if dispatcher == nil {
		t.Fatal("dispatcher is nil")
	}
}

func TestNewPartnerRelayRejectsMissingKafkaBrokers(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	_, _, err = newPartnerRelay(db, runtime.Config{
		PartnerEventsTopic:      "partner.events.v1",
		OutboxRelayBatchSize:    20,
		OutboxRelayPollInterval: time.Second,
		OutboxRelayLockTTL:      30 * time.Second,
		OutboxRelayRetryDelay:   5 * time.Second,
	}, "relay-1")
	if !errors.Is(err, kafkapub.ErrInvalidConfig) {
		t.Fatalf("newPartnerRelay() error = %v, want %v", err, kafkapub.ErrInvalidConfig)
	}
}
