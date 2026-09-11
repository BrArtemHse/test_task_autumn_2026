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
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/application"
	orderpostgres "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/postgres"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/kafkaconsume"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/kafkapub"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/runtime"
)

func TestNewOrderStoreRunsMigrationsBeforeReturningStore(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "0001_schema.sql"), []byte("CREATE SCHEMA IF NOT EXISTS orders;"), 0o600); err != nil {
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
		WithArgs("migrations:order-service").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT checksum FROM schema_migrations").
		WithArgs("order-service", "0001").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("CREATE SCHEMA IF NOT EXISTS orders;").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("INSERT INTO schema_migrations").
		WithArgs("order-service", "0001", "0001_schema.sql", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	store, err := newOrderStore(context.Background(), db, dir)
	if err != nil {
		t.Fatalf("newOrderStore() error = %v", err)
	}
	if store == nil {
		t.Fatal("store is nil")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestNewOrderRelayRejectsMissingKafkaBrokers(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	_, closer, err := newOrderRelay(db, runtime.Config{
		ServiceName:             "order-service",
		OrderEventsTopic:        "orders.events.v1",
		OutboxRelayBatchSize:    25,
		OutboxRelayPollInterval: time.Millisecond,
		OutboxRelayLockTTL:      30 * time.Second,
		OutboxRelayRetryDelay:   5 * time.Second,
	}, "relay-1")

	if closer != nil {
		t.Fatal("closer is not nil")
	}
	if !errors.Is(err, kafkapub.ErrInvalidConfig) {
		t.Fatalf("newOrderRelay() error = %v, want %v", err, kafkapub.ErrInvalidConfig)
	}
}

func TestNewOrderRelayUsesRuntimeConfigForClaims(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	relay, closer, err := newOrderRelay(db, runtime.Config{
		ServiceName:             "order-service",
		KafkaBrokers:            []string{"localhost:19092"},
		OrderEventsTopic:        "orders.events.v1",
		OutboxRelayBatchSize:    25,
		OutboxRelayPollInterval: time.Millisecond,
		OutboxRelayLockTTL:      30 * time.Second,
		OutboxRelayRetryDelay:   5 * time.Second,
	}, "relay-1")
	if err != nil {
		t.Fatalf("newOrderRelay() error = %v", err)
	}
	if relay == nil {
		t.Fatal("relay is nil")
	}
	if closer == nil {
		t.Fatal("closer is nil")
	}
	t.Cleanup(func() { _ = closer.Close() })

	mock.ExpectBegin()
	mock.ExpectQuery("UPDATE orders\\.outbox_messages").
		WithArgs("relay-1", int64(30), int64(25)).
		WillReturnRows(sqlmock.NewRows([]string{
			"id",
			"event_type",
			"event_version",
			"producer",
			"aggregate_type",
			"aggregate_id",
			"partition_key",
			"correlation_id",
			"causation_id",
			"payload",
			"occurred_at",
		}))
	mock.ExpectCommit()

	if err := relay.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestNewPartnerEventConsumerRejectsMissingKafkaBrokers(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	service := application.NewService(application.ServiceConfig{Store: orderpostgres.NewStore(db)})

	_, err = newPartnerEventConsumer(runtime.Config{
		PartnerEventsTopic:         "partner.events.v1",
		PartnerEventsConsumerGroup: "order-service-partner-events-v1",
		KafkaConsumerRetryDelay:    time.Second,
	}, service)
	if !errors.Is(err, kafkaconsume.ErrInvalidConfig) {
		t.Fatalf("newPartnerEventConsumer() error = %v, want %v", err, kafkaconsume.ErrInvalidConfig)
	}
}
