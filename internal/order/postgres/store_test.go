package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/application"
	orderdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/domain"
	orderpostgres "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/postgres"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/events"
)

const (
	advisoryLockSQL       = "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"
	selectIdempotencySQL  = "SELECT request_fingerprint, order_id FROM orders.idempotency_keys WHERE user_id = $1 AND idempotency_key = $2"
	insertOrderSQL        = "INSERT INTO orders.orders (id, user_id, restaurant_id, status, total_cents, version) VALUES ($1, $2, $3, $4, $5, $6)"
	insertOrderItemSQL    = "INSERT INTO orders.order_items (order_id, menu_item_id, name, unit_price_cents, quantity, modifier_item_ids, line_total_cents) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)"
	insertIdempotencySQL  = "INSERT INTO orders.idempotency_keys (user_id, idempotency_key, request_fingerprint, order_id) VALUES ($1, $2, $3, $4)"
	insertOutboxSQL       = "INSERT INTO orders.outbox_messages (id, event_type, event_version, producer, aggregate_type, aggregate_id, partition_key, correlation_id, causation_id, payload, occurred_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11)"
	selectOrderByIDSQL    = "SELECT id, user_id, restaurant_id, status, total_cents, version FROM orders.orders WHERE id = $1"
	selectOrderForUserSQL = "SELECT id, user_id, restaurant_id, status, total_cents, version FROM orders.orders WHERE id = $1 AND user_id = $2"
	selectOrderForUpdate  = "SELECT id, user_id, restaurant_id, status, total_cents, version FROM orders.orders WHERE id = $1 FOR UPDATE"
	selectOrderItemsSQL   = "SELECT menu_item_id, name, unit_price_cents, quantity, modifier_item_ids, line_total_cents FROM orders.order_items WHERE order_id = $1 ORDER BY id"
	selectInboxSQL        = "SELECT payload_fingerprint, aggregate_id FROM orders.inbox_messages WHERE event_id = $1"
	updateOrderStatusSQL  = "UPDATE orders.orders SET status = $2, version = $3, updated_at = now() WHERE id = $1"
	insertInboxSQL        = "INSERT INTO orders.inbox_messages (event_id, event_type, event_version, aggregate_id, payload_fingerprint) VALUES ($1, $2, $3, $4, $5)"
)

func TestStoreFindOrderByIdempotencyKey(t *testing.T) {
	dbError := errors.New("database offline")
	for _, tt := range []struct {
		name        string
		fingerprint string
		queryError  error
		wantFound   bool
		wantError   error
	}{
		{name: "missing", fingerprint: "fingerprint-1", queryError: sql.ErrNoRows},
		{name: "existing", fingerprint: "fingerprint-1", wantFound: true},
		{name: "conflict", fingerprint: "other", wantFound: true, wantError: application.ErrIdempotencyConflict},
		{name: "database error", fingerprint: "fingerprint-1", queryError: dbError, wantError: dbError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, mock, store := newMockStore(t)
			query := mock.ExpectQuery(selectIdempotencySQL).WithArgs("usr-1", "idem-1")
			if tt.queryError != nil {
				query.WillReturnError(tt.queryError)
			} else {
				query.WillReturnRows(sqlmock.NewRows([]string{"request_fingerprint", "order_id"}).
					AddRow("fingerprint-1", "ord-existing"))
			}
			if tt.wantFound && tt.wantError == nil {
				expectLoadOrderByID(mock, "ord-existing", string(orderdomain.StatusAccepted), 2)
			}
			result, found, err := store.FindOrderByIdempotencyKey(context.Background(), "usr-1", "idem-1", tt.fingerprint)
			if found != tt.wantFound || !errors.Is(err, tt.wantError) {
				t.Fatalf("found, error = %v, %v; want %v, %v", found, err, tt.wantFound, tt.wantError)
			}
			if found && err == nil {
				if !result.ReusedIdempotencyKey || result.Order.ID != "ord-existing" ||
					result.Order.Status != orderdomain.StatusAccepted || result.Order.TotalCents != 2400 ||
					len(result.Order.Items) != 1 || result.Order.Items[0].UnitPriceCents != 1200 {
					t.Fatalf("stored order snapshot = %#v", result)
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStoreCreateOrderPersistsOrderIdempotencyAndOutboxInOneTransaction(t *testing.T) {
	_, mock, store := newMockStore(t)
	record := createOrderRecord(t, "ord-1", "idem-1", "fingerprint-1", "evt-1")

	mock.ExpectBegin()
	expectAdvisoryLock(mock, "usr-1:idem-1")
	mock.ExpectQuery(selectIdempotencySQL).
		WithArgs("usr-1", "idem-1").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectExec(insertOrderSQL).
		WithArgs("ord-1", "usr-1", "rst-1", string(orderdomain.StatusPendingRestaurantConfirmation), int64(2400), int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(insertOrderItemSQL).
		WithArgs("ord-1", "pizza", "Pizza", int64(1200), int64(2), `["extra-cheese"]`, int64(2400)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(insertIdempotencySQL).
		WithArgs("usr-1", "idem-1", "fingerprint-1", "ord-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(insertOutboxSQL).
		WithArgs("evt-1", application.EventTypeOrderCreated, int64(1), "order-service", "order", "ord-1", "ord-1", "req-1", "idem-1", `{"order_id":"ord-1"}`, fixedTime()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := store.CreateOrder(context.Background(), record)
	if err != nil {
		t.Fatalf("CreateOrder() error = %v", err)
	}
	if result.ReusedIdempotencyKey {
		t.Fatal("ReusedIdempotencyKey = true, want false")
	}
	if result.Order.ID != "ord-1" {
		t.Fatalf("order id = %q, want ord-1", result.Order.ID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestStoreCreateOrderReusesSameIdempotencyKey(t *testing.T) {
	_, mock, store := newMockStore(t)
	record := createOrderRecord(t, "ord-new", "idem-1", "fingerprint-1", "evt-new")

	mock.ExpectBegin()
	expectAdvisoryLock(mock, "usr-1:idem-1")
	mock.ExpectQuery(selectIdempotencySQL).
		WithArgs("usr-1", "idem-1").
		WillReturnRows(sqlmock.NewRows([]string{"request_fingerprint", "order_id"}).AddRow("fingerprint-1", "ord-existing"))
	expectLoadOrderByID(mock, "ord-existing", string(orderdomain.StatusAccepted), int64(2))
	mock.ExpectCommit()

	result, err := store.CreateOrder(context.Background(), record)
	if err != nil {
		t.Fatalf("CreateOrder() error = %v", err)
	}
	if !result.ReusedIdempotencyKey {
		t.Fatal("ReusedIdempotencyKey = false, want true")
	}
	if result.Order.ID != "ord-existing" {
		t.Fatalf("order id = %q, want ord-existing", result.Order.ID)
	}
	if result.Order.Status != orderdomain.StatusAccepted {
		t.Fatalf("status = %q, want accepted", result.Order.Status)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestStoreCreateOrderRejectsSameIdempotencyKeyWithDifferentFingerprint(t *testing.T) {
	_, mock, store := newMockStore(t)
	record := createOrderRecord(t, "ord-new", "idem-1", "fingerprint-new", "evt-new")

	mock.ExpectBegin()
	expectAdvisoryLock(mock, "usr-1:idem-1")
	mock.ExpectQuery(selectIdempotencySQL).
		WithArgs("usr-1", "idem-1").
		WillReturnRows(sqlmock.NewRows([]string{"request_fingerprint", "order_id"}).AddRow("fingerprint-existing", "ord-existing"))
	mock.ExpectRollback()

	_, err := store.CreateOrder(context.Background(), record)
	if !errors.Is(err, application.ErrIdempotencyConflict) {
		t.Fatalf("CreateOrder() error = %v, want %v", err, application.ErrIdempotencyConflict)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestStoreGetOrderRequiresUserScope(t *testing.T) {
	_, mock, store := newMockStore(t)

	mock.ExpectQuery(selectOrderForUserSQL).
		WithArgs("ord-1", "wrong-user").
		WillReturnError(sql.ErrNoRows)

	_, err := store.GetOrder(context.Background(), "ord-1", "wrong-user")
	if !errors.Is(err, application.ErrOrderNotFound) {
		t.Fatalf("GetOrder() error = %v, want %v", err, application.ErrOrderNotFound)
	}

	mock.ExpectQuery(selectOrderForUserSQL).
		WithArgs("ord-1", "usr-1").
		WillReturnRows(orderRows().AddRow("ord-1", "usr-1", "rst-1", string(orderdomain.StatusAccepted), int64(2400), int64(2)))
	expectLoadItems(mock, "ord-1")

	order, err := store.GetOrder(context.Background(), "ord-1", "usr-1")
	if err != nil {
		t.Fatalf("GetOrder() error = %v", err)
	}
	if order.UserID != "usr-1" {
		t.Fatalf("user id = %q, want usr-1", order.UserID)
	}
	if len(order.Items) != 1 {
		t.Fatalf("items = %d, want 1", len(order.Items))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestStoreApplyPartnerEventTransitionsOrderAndStoresInbox(t *testing.T) {
	_, mock, store := newMockStore(t)

	mock.ExpectBegin()
	expectAdvisoryLock(mock, "evt-partner-1")
	mock.ExpectQuery(selectInboxSQL).
		WithArgs("evt-partner-1").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(selectOrderForUpdate).
		WithArgs("ord-1").
		WillReturnRows(orderRows().AddRow("ord-1", "usr-1", "rst-1", string(orderdomain.StatusPendingRestaurantConfirmation), int64(2400), int64(1)))
	expectLoadItems(mock, "ord-1")
	mock.ExpectExec(updateOrderStatusSQL).
		WithArgs("ord-1", string(orderdomain.StatusAccepted), int64(2)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(insertInboxSQL).
		WithArgs("evt-partner-1", application.EventTypePartnerOrderAccepted, int64(1), "ord-1", "payload-fingerprint-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	result, err := store.ApplyPartnerEvent(context.Background(), application.ApplyPartnerEventRecord{
		EventID:            "evt-partner-1",
		EventType:          application.EventTypePartnerOrderAccepted,
		OrderID:            "ord-1",
		NewStatus:          orderdomain.StatusAccepted,
		PayloadFingerprint: "payload-fingerprint-1",
	})
	if err != nil {
		t.Fatalf("ApplyPartnerEvent() error = %v", err)
	}
	if !result.Changed {
		t.Fatal("changed = false, want true")
	}
	if result.Order.Status != orderdomain.StatusAccepted {
		t.Fatalf("status = %q, want accepted", result.Order.Status)
	}
	if result.Order.Version != 2 {
		t.Fatalf("version = %d, want 2", result.Order.Version)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestStoreApplyPartnerEventDeduplicatesSameEvent(t *testing.T) {
	_, mock, store := newMockStore(t)

	mock.ExpectBegin()
	expectAdvisoryLock(mock, "evt-partner-1")
	mock.ExpectQuery(selectInboxSQL).
		WithArgs("evt-partner-1").
		WillReturnRows(sqlmock.NewRows([]string{"payload_fingerprint", "aggregate_id"}).AddRow("payload-fingerprint-1", "ord-1"))
	expectLoadOrderByID(mock, "ord-1", string(orderdomain.StatusAccepted), int64(2))
	mock.ExpectCommit()

	result, err := store.ApplyPartnerEvent(context.Background(), application.ApplyPartnerEventRecord{
		EventID:            "evt-partner-1",
		EventType:          application.EventTypePartnerOrderAccepted,
		OrderID:            "ord-1",
		NewStatus:          orderdomain.StatusAccepted,
		PayloadFingerprint: "payload-fingerprint-1",
	})
	if err != nil {
		t.Fatalf("ApplyPartnerEvent() error = %v", err)
	}
	if !result.Duplicate {
		t.Fatal("duplicate = false, want true")
	}
	if result.Changed {
		t.Fatal("changed = true, want false")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestStoreApplyPartnerEventRejectsSameEventWithDifferentPayloadFingerprint(t *testing.T) {
	_, mock, store := newMockStore(t)

	mock.ExpectBegin()
	expectAdvisoryLock(mock, "evt-partner-1")
	mock.ExpectQuery(selectInboxSQL).
		WithArgs("evt-partner-1").
		WillReturnRows(sqlmock.NewRows([]string{"payload_fingerprint", "aggregate_id"}).AddRow("payload-fingerprint-existing", "ord-1"))
	mock.ExpectRollback()

	_, err := store.ApplyPartnerEvent(context.Background(), application.ApplyPartnerEventRecord{
		EventID:            "evt-partner-1",
		EventType:          application.EventTypePartnerOrderAccepted,
		OrderID:            "ord-1",
		NewStatus:          orderdomain.StatusAccepted,
		PayloadFingerprint: "payload-fingerprint-new",
	})
	if !errors.Is(err, application.ErrInboxConflict) {
		t.Fatalf("ApplyPartnerEvent() error = %v, want %v", err, application.ErrInboxConflict)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func newMockStore(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *orderpostgres.Store) {
	t.Helper()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return db, mock, orderpostgres.NewStore(db)
}

func expectAdvisoryLock(mock sqlmock.Sqlmock, key string) {
	mock.ExpectExec(advisoryLockSQL).
		WithArgs(key).
		WillReturnResult(sqlmock.NewResult(0, 0))
}

func expectLoadOrderByID(mock sqlmock.Sqlmock, orderID, status string, version int64) {
	mock.ExpectQuery(selectOrderByIDSQL).
		WithArgs(orderID).
		WillReturnRows(orderRows().AddRow(orderID, "usr-1", "rst-1", status, int64(2400), version))
	expectLoadItems(mock, orderID)
}

func expectLoadItems(mock sqlmock.Sqlmock, orderID string) {
	mock.ExpectQuery(selectOrderItemsSQL).
		WithArgs(orderID).
		WillReturnRows(sqlmock.NewRows([]string{
			"menu_item_id",
			"name",
			"unit_price_cents",
			"quantity",
			"modifier_item_ids",
			"line_total_cents",
		}).AddRow("pizza", "Pizza", int64(1200), int64(2), `["extra-cheese"]`, int64(2400)))
}

func orderRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id",
		"user_id",
		"restaurant_id",
		"status",
		"total_cents",
		"version",
	})
}

func createOrderRecord(t *testing.T, orderID, idempotencyKey, fingerprint, eventID string) application.CreateOrderRecord {
	t.Helper()

	order, err := orderdomain.NewOrder(orderdomain.NewOrderInput{
		OrderID:      orderID,
		UserID:       "usr-1",
		RestaurantID: "rst-1",
		Items: []orderdomain.NewOrderItemInput{
			{
				MenuItemID:      "pizza",
				Name:            "Pizza",
				UnitPriceCents:  1200,
				Quantity:        2,
				ModifierItemIDs: []string{"extra-cheese"},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewOrder() error = %v", err)
	}

	event, err := events.NewEnvelope(events.NewEnvelopeInput{
		EventID:       eventID,
		EventType:     application.EventTypeOrderCreated,
		EventVersion:  1,
		OccurredAt:    fixedTime(),
		Producer:      "order-service",
		AggregateType: "order",
		AggregateID:   orderID,
		PartitionKey:  orderID,
		CorrelationID: "req-1",
		CausationID:   idempotencyKey,
		Payload:       []byte(`{"order_id":"` + orderID + `"}`),
	})
	if err != nil {
		t.Fatalf("NewEnvelope() error = %v", err)
	}

	return application.CreateOrderRecord{
		Order:              order,
		IdempotencyKey:     idempotencyKey,
		RequestFingerprint: fingerprint,
		OutboxEvent:        event,
	}
}

func fixedTime() time.Time {
	return time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
}
