package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	orderpostgres "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/postgres"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/outbox"
)

const (
	claimOutboxSQL = `UPDATE orders.outbox_messages
SET locked_by = $1,
    locked_until = now() + ($2 * interval '1 second'),
    publish_attempts = publish_attempts + 1,
    last_error = NULL
WHERE id IN (
    SELECT id
    FROM orders.outbox_messages
    WHERE published_at IS NULL
      AND (locked_until IS NULL OR locked_until < now())
      AND (next_attempt_at IS NULL OR next_attempt_at <= now())
    ORDER BY occurred_at, id
    LIMIT $3
    FOR UPDATE SKIP LOCKED
)
RETURNING id, event_type, event_version, producer, aggregate_type, aggregate_id, partition_key, correlation_id, causation_id, payload::text, occurred_at`
	markOutboxPublishedSQL = `UPDATE orders.outbox_messages
SET published_at = now(),
    locked_by = NULL,
    locked_until = NULL,
    next_attempt_at = NULL,
    last_error = NULL
WHERE id = $1
  AND locked_by = $2
  AND published_at IS NULL`
	markOutboxFailedSQL = `UPDATE orders.outbox_messages
SET locked_by = NULL,
    locked_until = NULL,
    last_error = $3,
    next_attempt_at = now() + ($4 * interval '1 second')
WHERE id = $1
  AND locked_by = $2
  AND published_at IS NULL`
)

func TestOutboxStoreClaimLocksDueUnpublishedRows(t *testing.T) {
	_, mock, store := newMockOutboxStore(t)

	mock.ExpectBegin()
	mock.ExpectQuery(claimOutboxSQL).
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
		}).AddRow(
			"evt-1",
			"order.created.v1",
			int64(1),
			"order-service",
			"order",
			"ord-1",
			"ord-1",
			"req-1",
			"idem-1",
			[]byte(`{"order_id":"ord-1"}`),
			fixedTime(),
		))
	mock.ExpectCommit()

	messages, err := store.Claim(context.Background(), 25)
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}

	want := []outbox.Message{
		{
			EventID: "evt-1",
			Topic:   "orders.events.v1",
			Key:     "ord-1",
			Value:   []byte(`{"order_id":"ord-1"}`),
			Headers: map[string]string{
				"aggregate_id":   "ord-1",
				"aggregate_type": "order",
				"causation_id":   "idem-1",
				"correlation_id": "req-1",
				"event_id":       "evt-1",
				"event_type":     "order.created.v1",
				"event_version":  "1",
				"occurred_at":    fixedTime().Format(time.RFC3339Nano),
				"producer":       "order-service",
			},
		},
	}
	if !reflect.DeepEqual(messages, want) {
		t.Fatalf("messages = %#v, want %#v", messages, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestOutboxStoreMarkPublishedRequiresRelayClaim(t *testing.T) {
	_, mock, store := newMockOutboxStore(t)

	mock.ExpectExec(markOutboxPublishedSQL).
		WithArgs("evt-1", "relay-1").
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := store.MarkPublished(context.Background(), "evt-1")
	if !errors.Is(err, orderpostgres.ErrOutboxMessageNotOwned) {
		t.Fatalf("MarkPublished() error = %v, want %v", err, orderpostgres.ErrOutboxMessageNotOwned)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestOutboxStoreMarkFailedReleasesClaimAndSchedulesRetry(t *testing.T) {
	_, mock, store := newMockOutboxStore(t)
	publishErr := errors.New("broker unavailable")

	mock.ExpectExec(markOutboxFailedSQL).
		WithArgs("evt-1", "relay-1", "broker unavailable", int64(5)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := store.MarkFailed(context.Background(), "evt-1", publishErr); err != nil {
		t.Fatalf("MarkFailed() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestNewOutboxStoreRejectsInvalidConfig(t *testing.T) {
	db, _, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	tests := []struct {
		name   string
		db     *sql.DB
		config orderpostgres.OutboxConfig
	}{
		{
			name: "missing database",
			db:   nil,
			config: orderpostgres.OutboxConfig{
				RelayID:    "relay-1",
				Topic:      "orders.events.v1",
				LockTTL:    30 * time.Second,
				RetryDelay: 5 * time.Second,
			},
		},
		{
			name: "missing relay id",
			db:   db,
			config: orderpostgres.OutboxConfig{
				Topic:      "orders.events.v1",
				LockTTL:    30 * time.Second,
				RetryDelay: 5 * time.Second,
			},
		},
		{
			name: "missing topic",
			db:   db,
			config: orderpostgres.OutboxConfig{
				RelayID:    "relay-1",
				LockTTL:    30 * time.Second,
				RetryDelay: 5 * time.Second,
			},
		},
		{
			name: "missing lock ttl",
			db:   db,
			config: orderpostgres.OutboxConfig{
				RelayID:    "relay-1",
				Topic:      "orders.events.v1",
				RetryDelay: 5 * time.Second,
			},
		},
		{
			name: "missing retry delay",
			db:   db,
			config: orderpostgres.OutboxConfig{
				RelayID: "relay-1",
				Topic:   "orders.events.v1",
				LockTTL: 30 * time.Second,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := orderpostgres.NewOutboxStore(tt.db, tt.config)
			if !errors.Is(err, orderpostgres.ErrInvalidOutboxConfig) {
				t.Fatalf("NewOutboxStore() error = %v, want %v", err, orderpostgres.ErrInvalidOutboxConfig)
			}
		})
	}
}

func newMockOutboxStore(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *orderpostgres.OutboxStore) {
	t.Helper()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	store, err := orderpostgres.NewOutboxStore(db, orderpostgres.OutboxConfig{
		RelayID:    "relay-1",
		Topic:      "orders.events.v1",
		LockTTL:    30 * time.Second,
		RetryDelay: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewOutboxStore() error = %v", err)
	}

	return db, mock, store
}
