package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/contracts/orderevents"
	partnerapp "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/application"
	partnerpostgres "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/postgres"
)

const (
	partnerAdvisoryLockSQL = "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"
	selectPartnerInboxSQL  = "SELECT payload_fingerprint, aggregate_id FROM partner.inbox_messages WHERE event_id = $1"
	selectIntegrationSQL   = "SELECT external_store_id, callback_url FROM partner.restaurant_integrations WHERE restaurant_id = $1 AND status = 'active'"
	insertSubmissionSQL    = "INSERT INTO partner.partner_order_submissions (id, order_id, restaurant_id, external_store_id, destination_url, status, payload, correlation_id, source_event_id) VALUES ($1, $2, $3, $4, $5, 'pending', $6::jsonb, $7, $8)"
	insertPartnerInboxSQL  = "INSERT INTO partner.inbox_messages (event_id, event_type, event_version, aggregate_id, payload_fingerprint) VALUES ($1, $2, $3, $4, $5)"
)

func TestStoreReceiveOrderPersistsSubmissionAndInboxInOneTransaction(t *testing.T) {
	_, mock, store := newPartnerStore(t)
	record := receiveRecord()

	mock.ExpectBegin()
	mock.ExpectExec(partnerAdvisoryLockSQL).
		WithArgs("evt-1").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(selectPartnerInboxSQL).
		WithArgs("evt-1").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(selectIntegrationSQL).
		WithArgs("rst-pizza-1").
		WillReturnRows(sqlmock.NewRows([]string{"external_store_id", "callback_url"}).
			AddRow("store-pizza-1", "http://sample-restaurant-service:8084/orders"))
	mock.ExpectExec(insertSubmissionSQL).
		WithArgs(
			"sub-1",
			"ord-1",
			"rst-pizza-1",
			"store-pizza-1",
			"http://sample-restaurant-service:8084/orders",
			`{"order_id":"ord-1","user_id":"usr-1","restaurant_id":"rst-pizza-1","status":"pending_restaurant_confirmation","total_cents":69000,"items":[{"menu_item_id":"item-margherita","name":"Margherita","unit_price_cents":69000,"quantity":1,"modifier_item_ids":[],"line_total_cents":69000}]}`,
			"req-1",
			"evt-1",
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(insertPartnerInboxSQL).
		WithArgs("evt-1", partnerapp.EventTypeOrderCreated, int64(1), "ord-1", "fingerprint-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := store.ReceiveOrder(context.Background(), record)
	if err != nil {
		t.Fatalf("ReceiveOrder() error = %v", err)
	}
	if result.SubmissionID != "sub-1" || result.Duplicate {
		t.Fatalf("result = %#v", result)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestStoreReceiveOrderDeduplicatesSameEvent(t *testing.T) {
	_, mock, store := newPartnerStore(t)

	mock.ExpectBegin()
	mock.ExpectExec(partnerAdvisoryLockSQL).
		WithArgs("evt-1").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(selectPartnerInboxSQL).
		WithArgs("evt-1").
		WillReturnRows(sqlmock.NewRows([]string{"payload_fingerprint", "aggregate_id"}).
			AddRow("fingerprint-1", "ord-1"))
	mock.ExpectCommit()

	result, err := store.ReceiveOrder(context.Background(), receiveRecord())
	if err != nil {
		t.Fatalf("ReceiveOrder() error = %v", err)
	}
	if !result.Duplicate {
		t.Fatal("duplicate = false, want true")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestStoreReceiveOrderRejectsConflictingEventIdentity(t *testing.T) {
	_, mock, store := newPartnerStore(t)
	record := receiveRecord()
	record.PayloadFingerprint = "fingerprint-new"

	mock.ExpectBegin()
	mock.ExpectExec(partnerAdvisoryLockSQL).
		WithArgs("evt-1").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(selectPartnerInboxSQL).
		WithArgs("evt-1").
		WillReturnRows(sqlmock.NewRows([]string{"payload_fingerprint", "aggregate_id"}).
			AddRow("fingerprint-existing", "ord-1"))
	mock.ExpectRollback()

	_, err := store.ReceiveOrder(context.Background(), record)
	if !errors.Is(err, partnerapp.ErrInboxConflict) {
		t.Fatalf("ReceiveOrder() error = %v, want %v", err, partnerapp.ErrInboxConflict)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestStoreReceiveOrderRequiresActiveIntegration(t *testing.T) {
	_, mock, store := newPartnerStore(t)

	mock.ExpectBegin()
	mock.ExpectExec(partnerAdvisoryLockSQL).
		WithArgs("evt-1").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(selectPartnerInboxSQL).
		WithArgs("evt-1").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(selectIntegrationSQL).
		WithArgs("rst-pizza-1").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	_, err := store.ReceiveOrder(context.Background(), receiveRecord())
	if !errors.Is(err, partnerapp.ErrIntegrationNotFound) {
		t.Fatalf("ReceiveOrder() error = %v, want %v", err, partnerapp.ErrIntegrationNotFound)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func newPartnerStore(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *partnerpostgres.Store) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, mock, partnerpostgres.NewStore(db)
}

func receiveRecord() partnerapp.ReceiveOrderRecord {
	return partnerapp.ReceiveOrderRecord{
		SubmissionID:       "sub-1",
		EventID:            "evt-1",
		EventType:          partnerapp.EventTypeOrderCreated,
		EventVersion:       1,
		CorrelationID:      "req-1",
		OrderID:            "ord-1",
		RestaurantID:       "rst-pizza-1",
		PayloadFingerprint: "fingerprint-1",
		Payload: orderevents.OrderCreatedV1{
			OrderID:      "ord-1",
			UserID:       "usr-1",
			RestaurantID: "rst-pizza-1",
			Status:       orderevents.PendingStatus,
			TotalCents:   69000,
			Items: []orderevents.OrderCreatedItemV1{{
				MenuItemID:      "item-margherita",
				Name:            "Margherita",
				UnitPriceCents:  69000,
				Quantity:        1,
				ModifierItemIDs: []string{},
				LineTotalCents:  69000,
			}},
		},
	}
}
