package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	partnerpostgres "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/postgres"
)

const (
	claimPartnerOutboxSQL = `UPDATE partner.outbox_messages
SET locked_by = $1,
    locked_until = now() + ($2 * interval '1 second'),
    publish_attempts = publish_attempts + 1,
    last_error = NULL
WHERE id IN (
    SELECT id
    FROM partner.outbox_messages
    WHERE published_at IS NULL
      AND (locked_until IS NULL OR locked_until < now())
      AND (next_attempt_at IS NULL OR next_attempt_at <= now())
    ORDER BY occurred_at, id
    LIMIT $3
    FOR UPDATE SKIP LOCKED
)
RETURNING id, event_type, event_version, producer, aggregate_type, aggregate_id, partition_key, correlation_id, causation_id, payload::text, occurred_at`
	markPartnerOutboxPublishedSQL = `UPDATE partner.outbox_messages
SET published_at = now(),
    locked_by = NULL,
    locked_until = NULL,
    next_attempt_at = NULL,
    last_error = NULL
WHERE id = $1
  AND locked_by = $2
  AND published_at IS NULL`
	markPartnerOutboxFailedSQL = `UPDATE partner.outbox_messages
SET locked_by = NULL,
    locked_until = NULL,
    last_error = $3,
    next_attempt_at = now() + ($4 * interval '1 second')
WHERE id = $1
  AND locked_by = $2
  AND published_at IS NULL`
)

func TestPartnerOutboxStoreClaimsMessagesWithLease(t *testing.T) {
	db, mock, store := newPartnerOutboxStore(t)
	defer func() { _ = db.Close() }()

	mock.ExpectBegin()
	mock.ExpectQuery(claimPartnerOutboxSQL).
		WithArgs("relay-1", int64(30), int64(10)).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "event_type", "event_version", "producer", "aggregate_type", "aggregate_id",
			"partition_key", "correlation_id", "causation_id", "payload", "occurred_at",
		}).AddRow(
			"event-1", "partner.order_accepted.v1", int64(1), "partner-service", "order", "order-1",
			"order-1", "request-1", "callback-1", `{"order_id":"order-1","status":"accepted"}`, fixedPartnerCallbackTime(),
		))
	mock.ExpectCommit()

	messages, err := store.Claim(context.Background(), 10)
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("messages = %d, want 1", len(messages))
	}
	message := messages[0]
	if message.EventID != "event-1" || message.Topic != "partner.events.v1" || message.Key != "order-1" {
		t.Fatalf("message routing = %#v", message)
	}
	if message.Headers["event_type"] != "partner.order_accepted.v1" || message.Headers["producer"] != "partner-service" {
		t.Fatalf("message headers = %#v", message.Headers)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestPartnerOutboxStoreMarksPublishedOnlyWhenOwned(t *testing.T) {
	db, mock, store := newPartnerOutboxStore(t)
	defer func() { _ = db.Close() }()

	mock.ExpectExec(markPartnerOutboxPublishedSQL).
		WithArgs("event-1", "relay-1").
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := store.MarkPublished(context.Background(), "event-1")
	if !errors.Is(err, partnerpostgres.ErrPartnerOutboxMessageNotOwned) {
		t.Fatalf("MarkPublished() error = %v, want ownership error", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestPartnerOutboxStoreSchedulesRetry(t *testing.T) {
	db, mock, store := newPartnerOutboxStore(t)
	defer func() { _ = db.Close() }()

	mock.ExpectExec(markPartnerOutboxFailedSQL).
		WithArgs("event-1", "relay-1", "broker unavailable", int64(5)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	if err := store.MarkFailed(context.Background(), "event-1", errors.New("broker unavailable")); err != nil {
		t.Fatalf("MarkFailed() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func newPartnerOutboxStore(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *partnerpostgres.OutboxStore) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	store, err := partnerpostgres.NewOutboxStore(db, partnerpostgres.OutboxConfig{
		RelayID:    "relay-1",
		Topic:      "partner.events.v1",
		LockTTL:    30 * time.Second,
		RetryDelay: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewOutboxStore() error = %v", err)
	}
	return db, mock, store
}
