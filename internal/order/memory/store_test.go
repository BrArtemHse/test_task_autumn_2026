package memory

import (
	"context"
	"errors"
	"testing"

	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/application"
	orderdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/domain"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/events"
)

func TestStoreCreateOrderPersistsOrderAndOutboxEvent(t *testing.T) {
	store := NewStore()
	record := createOrderRecord(t, "ord-1", "idem-1", "fingerprint-1", "evt-1")

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

	outbox := store.OutboxEvents()
	if len(outbox) != 1 {
		t.Fatalf("outbox events = %d, want 1", len(outbox))
	}
	if outbox[0].EventID != "evt-1" {
		t.Fatalf("event id = %q, want evt-1", outbox[0].EventID)
	}
}

func TestStoreCreateOrderReusesIdempotencyKeyWithSameFingerprint(t *testing.T) {
	store := NewStore()
	first := createOrderRecord(t, "ord-1", "idem-1", "fingerprint-1", "evt-1")
	second := createOrderRecord(t, "ord-2", "idem-1", "fingerprint-1", "evt-2")

	if _, err := store.CreateOrder(context.Background(), first); err != nil {
		t.Fatalf("first CreateOrder() error = %v", err)
	}
	result, err := store.CreateOrder(context.Background(), second)
	if err != nil {
		t.Fatalf("second CreateOrder() error = %v", err)
	}

	if !result.ReusedIdempotencyKey {
		t.Fatal("ReusedIdempotencyKey = false, want true")
	}
	if result.Order.ID != "ord-1" {
		t.Fatalf("order id = %q, want original ord-1", result.Order.ID)
	}
	if outbox := store.OutboxEvents(); len(outbox) != 1 {
		t.Fatalf("outbox events = %d, want 1", len(outbox))
	}
}

func TestStoreCreateOrderRejectsIdempotencyKeyWithDifferentFingerprint(t *testing.T) {
	store := NewStore()
	first := createOrderRecord(t, "ord-1", "idem-1", "fingerprint-1", "evt-1")
	second := createOrderRecord(t, "ord-2", "idem-1", "fingerprint-2", "evt-2")

	if _, err := store.CreateOrder(context.Background(), first); err != nil {
		t.Fatalf("first CreateOrder() error = %v", err)
	}
	_, err := store.CreateOrder(context.Background(), second)
	if !errors.Is(err, application.ErrIdempotencyConflict) {
		t.Fatalf("second CreateOrder() error = %v, want %v", err, application.ErrIdempotencyConflict)
	}
	if outbox := store.OutboxEvents(); len(outbox) != 1 {
		t.Fatalf("outbox events = %d, want 1", len(outbox))
	}
}

func TestStoreGetOrderRequiresSameUser(t *testing.T) {
	store := NewStore()
	record := createOrderRecord(t, "ord-1", "idem-1", "fingerprint-1", "evt-1")
	if _, err := store.CreateOrder(context.Background(), record); err != nil {
		t.Fatalf("CreateOrder() error = %v", err)
	}

	_, err := store.GetOrder(context.Background(), "ord-1", "another-user")
	if !errors.Is(err, application.ErrOrderNotFound) {
		t.Fatalf("GetOrder() error = %v, want %v", err, application.ErrOrderNotFound)
	}

	order, err := store.GetOrder(context.Background(), "ord-1", "usr-1")
	if err != nil {
		t.Fatalf("GetOrder() same user error = %v", err)
	}
	if order.ID != "ord-1" {
		t.Fatalf("order id = %q, want ord-1", order.ID)
	}
}

func TestStoreApplyPartnerEventTransitionsOrderAndStoresInbox(t *testing.T) {
	store := NewStore()
	if _, err := store.CreateOrder(context.Background(), createOrderRecord(t, "ord-1", "idem-1", "fingerprint-1", "evt-1")); err != nil {
		t.Fatalf("CreateOrder() error = %v", err)
	}

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
}

func TestStoreApplyPartnerEventDeduplicatesSameEvent(t *testing.T) {
	store := NewStore()
	if _, err := store.CreateOrder(context.Background(), createOrderRecord(t, "ord-1", "idem-1", "fingerprint-1", "evt-1")); err != nil {
		t.Fatalf("CreateOrder() error = %v", err)
	}
	record := application.ApplyPartnerEventRecord{
		EventID:            "evt-partner-1",
		EventType:          application.EventTypePartnerOrderAccepted,
		OrderID:            "ord-1",
		NewStatus:          orderdomain.StatusAccepted,
		PayloadFingerprint: "payload-fingerprint-1",
	}

	if _, err := store.ApplyPartnerEvent(context.Background(), record); err != nil {
		t.Fatalf("first ApplyPartnerEvent() error = %v", err)
	}
	result, err := store.ApplyPartnerEvent(context.Background(), record)
	if err != nil {
		t.Fatalf("second ApplyPartnerEvent() error = %v", err)
	}

	if !result.Duplicate {
		t.Fatal("duplicate = false, want true")
	}
	if result.Changed {
		t.Fatal("changed = true, want false")
	}
	if result.Order.Version != 2 {
		t.Fatalf("version = %d, want 2", result.Order.Version)
	}
}

func TestStoreApplyPartnerEventRejectsSameEventWithDifferentPayloadFingerprint(t *testing.T) {
	store := NewStore()
	if _, err := store.CreateOrder(context.Background(), createOrderRecord(t, "ord-1", "idem-1", "fingerprint-1", "evt-1")); err != nil {
		t.Fatalf("CreateOrder() error = %v", err)
	}
	first := application.ApplyPartnerEventRecord{
		EventID:            "evt-partner-1",
		EventType:          application.EventTypePartnerOrderAccepted,
		OrderID:            "ord-1",
		NewStatus:          orderdomain.StatusAccepted,
		PayloadFingerprint: "payload-fingerprint-1",
	}
	second := first
	second.PayloadFingerprint = "payload-fingerprint-2"

	if _, err := store.ApplyPartnerEvent(context.Background(), first); err != nil {
		t.Fatalf("first ApplyPartnerEvent() error = %v", err)
	}
	_, err := store.ApplyPartnerEvent(context.Background(), second)
	if !errors.Is(err, application.ErrInboxConflict) {
		t.Fatalf("second ApplyPartnerEvent() error = %v, want %v", err, application.ErrInboxConflict)
	}
}

func createOrderRecord(t *testing.T, orderID, idempotencyKey, fingerprint, eventID string) application.CreateOrderRecord {
	t.Helper()

	order, err := orderdomain.NewOrder(orderdomain.NewOrderInput{
		OrderID:      orderID,
		UserID:       "usr-1",
		RestaurantID: "rst-1",
		Items: []orderdomain.NewOrderItemInput{
			{MenuItemID: "pizza", Name: "Pizza", UnitPriceCents: 1200, Quantity: 1},
		},
	})
	if err != nil {
		t.Fatalf("NewOrder() error = %v", err)
	}

	event, err := events.NewEnvelope(events.NewEnvelopeInput{
		EventID:       eventID,
		EventType:     application.EventTypeOrderCreated,
		EventVersion:  1,
		Producer:      "order-service",
		AggregateType: "order",
		AggregateID:   orderID,
		PartitionKey:  orderID,
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
