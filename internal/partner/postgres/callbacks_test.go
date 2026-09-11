package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/contracts/partnerevents"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/callback"
	partnerpostgres "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/postgres"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/events"
)

const (
	selectCallbackIntegrationSQL = "SELECT shared_secret_hash FROM partner.restaurant_integrations WHERE restaurant_id = $1 AND external_store_id = $2 AND status = 'active' FOR SHARE"
	selectCallbackSubmissionSQL  = "SELECT status FROM partner.partner_order_submissions WHERE order_id = $1 AND restaurant_id = $2 AND external_store_id = $3 FOR UPDATE"
	selectCallbackSQL            = "SELECT order_id, external_store_id, payload_fingerprint FROM partner.partner_callbacks WHERE id = $1"
	insertCallbackSQL            = "INSERT INTO partner.partner_callbacks (id, order_id, external_store_id, callback_type, payload, payload_fingerprint, received_at) VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)"
	insertCallbackOutboxSQL      = "INSERT INTO partner.outbox_messages (id, event_type, event_version, producer, aggregate_type, aggregate_id, partition_key, correlation_id, causation_id, payload, occurred_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11)"
	updateCallbackSubmissionSQL  = "UPDATE partner.partner_order_submissions SET status = $4, updated_at = now() WHERE order_id = $1 AND restaurant_id = $2 AND external_store_id = $3"
)

func TestCallbackStorePersistsCallbackAndOutboxInOneTransaction(t *testing.T) {
	_, mock, store := newCallbackStore(t)
	record := callbackRecord(t)

	mock.ExpectBegin()
	mock.ExpectExec(partnerAdvisoryLockSQL).
		WithArgs("callback-1").
		WillReturnResult(sqlmock.NewResult(0, 0))
	expectCallbackAuthority(mock, record.TokenDigest, "submitted")
	mock.ExpectQuery(selectCallbackSQL).
		WithArgs("callback-1").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(insertCallbackSQL).
		WithArgs(
			"callback-1",
			"order-1",
			"store-1",
			"accepted",
			`{"callback_id":"callback-1","order_id":"order-1","restaurant_id":"restaurant-1","external_store_id":"store-1","status":"accepted","partner_occurred_at":"2026-09-07T12:00:00Z"}`,
			"fingerprint-1",
			fixedPartnerCallbackTime(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(insertCallbackOutboxSQL).
		WithArgs(
			"event-1",
			partnerevents.OrderAcceptedV1Type,
			int64(1),
			"partner-service",
			"order",
			"order-1",
			"order-1",
			"request-1",
			"callback-1",
			`{"callback_id":"callback-1","order_id":"order-1","restaurant_id":"restaurant-1","external_store_id":"store-1","status":"accepted","partner_occurred_at":"2026-09-07T12:00:00Z"}`,
			fixedPartnerCallbackTime(),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(updateCallbackSubmissionSQL).
		WithArgs("order-1", "restaurant-1", "store-1", "accepted").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := store.ReceiveStatusCallback(context.Background(), record)
	if err != nil {
		t.Fatalf("ReceiveStatusCallback() error = %v", err)
	}
	if result.Duplicate {
		t.Fatal("duplicate = true, want false")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestCallbackStoreDeduplicatesSameAuthenticatedCallback(t *testing.T) {
	_, mock, store := newCallbackStore(t)
	record := callbackRecord(t)

	mock.ExpectBegin()
	mock.ExpectExec(partnerAdvisoryLockSQL).
		WithArgs("callback-1").
		WillReturnResult(sqlmock.NewResult(0, 0))
	expectCallbackAuthority(mock, record.TokenDigest, "accepted")
	mock.ExpectQuery(selectCallbackSQL).
		WithArgs("callback-1").
		WillReturnRows(sqlmock.NewRows([]string{"order_id", "external_store_id", "payload_fingerprint"}).
			AddRow("order-1", "store-1", "fingerprint-1"))
	mock.ExpectCommit()

	result, err := store.ReceiveStatusCallback(context.Background(), record)
	if err != nil {
		t.Fatalf("ReceiveStatusCallback() error = %v", err)
	}
	if !result.Duplicate {
		t.Fatal("duplicate = false, want true")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestCallbackStoreRejectsInvalidToken(t *testing.T) {
	_, mock, store := newCallbackStore(t)
	record := callbackRecord(t)

	mock.ExpectBegin()
	mock.ExpectExec(partnerAdvisoryLockSQL).
		WithArgs("callback-1").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(selectCallbackIntegrationSQL).
		WithArgs("restaurant-1", "store-1").
		WillReturnRows(sqlmock.NewRows([]string{"shared_secret_hash"}).AddRow("different-digest"))
	mock.ExpectRollback()

	_, err := store.ReceiveStatusCallback(context.Background(), record)
	if !errors.Is(err, callback.ErrUnauthorizedCallback) {
		t.Fatalf("ReceiveStatusCallback() error = %v, want %v", err, callback.ErrUnauthorizedCallback)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestCallbackStoreRejectsConflictingCallbackIdentity(t *testing.T) {
	_, mock, store := newCallbackStore(t)
	record := callbackRecord(t)

	mock.ExpectBegin()
	mock.ExpectExec(partnerAdvisoryLockSQL).
		WithArgs("callback-1").
		WillReturnResult(sqlmock.NewResult(0, 0))
	expectCallbackAuthority(mock, record.TokenDigest, "submitted")
	mock.ExpectQuery(selectCallbackSQL).
		WithArgs("callback-1").
		WillReturnRows(sqlmock.NewRows([]string{"order_id", "external_store_id", "payload_fingerprint"}).
			AddRow("order-1", "store-1", "different-fingerprint"))
	mock.ExpectRollback()

	_, err := store.ReceiveStatusCallback(context.Background(), record)
	if !errors.Is(err, callback.ErrCallbackConflict) {
		t.Fatalf("ReceiveStatusCallback() error = %v, want %v", err, callback.ErrCallbackConflict)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestCallbackStoreRejectsOrderNotSubmittedByAuthenticatedIntegration(t *testing.T) {
	_, mock, store := newCallbackStore(t)
	record := callbackRecord(t)

	mock.ExpectBegin()
	mock.ExpectExec(partnerAdvisoryLockSQL).
		WithArgs("callback-1").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(selectCallbackIntegrationSQL).
		WithArgs("restaurant-1", "store-1").
		WillReturnRows(sqlmock.NewRows([]string{"shared_secret_hash"}).AddRow(record.TokenDigest))
	mock.ExpectQuery(selectCallbackSubmissionSQL).
		WithArgs("order-1", "restaurant-1", "store-1").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	_, err := store.ReceiveStatusCallback(context.Background(), record)
	if !errors.Is(err, callback.ErrUnauthorizedCallback) {
		t.Fatalf("ReceiveStatusCallback() error = %v, want %v", err, callback.ErrUnauthorizedCallback)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestCallbackStoreRejectsOppositeTerminalDecision(t *testing.T) {
	_, mock, store := newCallbackStore(t)
	record := callbackRecord(t)
	record.Payload.Status = "rejected"

	mock.ExpectBegin()
	mock.ExpectExec(partnerAdvisoryLockSQL).
		WithArgs("callback-1").
		WillReturnResult(sqlmock.NewResult(0, 0))
	expectCallbackAuthority(mock, record.TokenDigest, "accepted")
	mock.ExpectRollback()

	_, err := store.ReceiveStatusCallback(context.Background(), record)
	if !errors.Is(err, callback.ErrCallbackConflict) {
		t.Fatalf("ReceiveStatusCallback() error = %v, want %v", err, callback.ErrCallbackConflict)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func newCallbackStore(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *partnerpostgres.CallbackStore) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, mock, partnerpostgres.NewCallbackStore(db)
}

func expectCallbackAuthority(mock sqlmock.Sqlmock, tokenDigest, submissionStatus string) {
	mock.ExpectQuery(selectCallbackIntegrationSQL).
		WithArgs("restaurant-1", "store-1").
		WillReturnRows(sqlmock.NewRows([]string{"shared_secret_hash"}).AddRow(tokenDigest))
	mock.ExpectQuery(selectCallbackSubmissionSQL).
		WithArgs("order-1", "restaurant-1", "store-1").
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow(submissionStatus))
}

func callbackRecord(t *testing.T) callback.ReceiveRecord {
	t.Helper()
	payload := partnerevents.OrderStatusChangedV1{
		CallbackID:        "callback-1",
		OrderID:           "order-1",
		RestaurantID:      "restaurant-1",
		ExternalStoreID:   "store-1",
		Status:            partnerevents.AcceptedStatus,
		PartnerOccurredAt: fixedPartnerCallbackTime(),
	}
	event, err := events.NewEnvelope(events.NewEnvelopeInput{
		EventID:       "event-1",
		EventType:     partnerevents.OrderAcceptedV1Type,
		EventVersion:  1,
		OccurredAt:    fixedPartnerCallbackTime(),
		Producer:      "partner-service",
		AggregateType: "order",
		AggregateID:   "order-1",
		PartitionKey:  "order-1",
		CorrelationID: "request-1",
		CausationID:   "callback-1",
		Payload:       []byte(`{"callback_id":"callback-1","order_id":"order-1","restaurant_id":"restaurant-1","external_store_id":"store-1","status":"accepted","partner_occurred_at":"2026-09-07T12:00:00Z"}`),
	})
	if err != nil {
		t.Fatalf("NewEnvelope() error = %v", err)
	}
	return callback.ReceiveRecord{
		CallbackID:         "callback-1",
		TokenDigest:        "token-digest-1",
		PayloadFingerprint: "fingerprint-1",
		Payload:            payload,
		OutboxEvent:        event,
	}
}

func fixedPartnerCallbackTime() time.Time {
	return time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
}
