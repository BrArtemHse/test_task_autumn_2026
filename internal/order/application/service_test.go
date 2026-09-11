package application

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	orderdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/domain"
)

func TestServiceCreateOrderValidatesCatalogAndStoresOutboxEvent(t *testing.T) {
	catalog := &catalogStub{
		items: []orderdomain.NewOrderItemInput{
			{MenuItemID: "pizza", Name: "Pizza", UnitPriceCents: 1200, Quantity: 2},
			{MenuItemID: "tea", Name: "Tea", UnitPriceCents: 250, Quantity: 1},
		},
	}
	store := &storeStub{}
	service := NewService(ServiceConfig{
		Catalog: catalog,
		Store:   store,
		NewID:   sequenceID("ord-1", "evt-1"),
		Now:     fixedTime,
	})

	result, err := service.CreateOrder(context.Background(), CreateOrderInput{
		RequestID:      "req-1",
		UserID:         "usr-1",
		IdempotencyKey: "idem-1",
		RestaurantID:   "rst-1",
		Items: []RequestedItem{
			{MenuItemID: "pizza", Quantity: 2},
			{MenuItemID: "tea", Quantity: 1},
		},
	})
	if err != nil {
		t.Fatalf("CreateOrder() error = %v", err)
	}

	if result.ReusedIdempotencyKey {
		t.Fatal("ReusedIdempotencyKey = true, want false")
	}
	if result.Order.ID != "ord-1" {
		t.Fatalf("order id = %q, want ord-1", result.Order.ID)
	}
	if result.Order.TotalCents != 2650 {
		t.Fatalf("total = %d, want 2650", result.Order.TotalCents)
	}

	if catalog.restaurantID != "rst-1" {
		t.Fatalf("catalog restaurant id = %q, want rst-1", catalog.restaurantID)
	}
	if len(catalog.requestedItems) != 2 {
		t.Fatalf("catalog requested items = %d, want 2", len(catalog.requestedItems))
	}

	if store.record.IdempotencyKey != "idem-1" {
		t.Fatalf("idempotency key = %q, want idem-1", store.record.IdempotencyKey)
	}
	if store.record.RequestFingerprint == "" {
		t.Fatal("request fingerprint is empty")
	}
	if store.record.Order.Status != orderdomain.StatusPendingRestaurantConfirmation {
		t.Fatalf("stored order status = %q", store.record.Order.Status)
	}

	event := store.record.OutboxEvent
	if event.EventID != "evt-1" {
		t.Fatalf("event id = %q, want evt-1", event.EventID)
	}
	if event.EventType != EventTypeOrderCreated {
		t.Fatalf("event type = %q, want %q", event.EventType, EventTypeOrderCreated)
	}
	if event.EventVersion != 1 {
		t.Fatalf("event version = %d, want 1", event.EventVersion)
	}
	if event.AggregateID != "ord-1" || event.PartitionKey != "ord-1" {
		t.Fatalf("event aggregate/partition = %q/%q, want ord-1/ord-1", event.AggregateID, event.PartitionKey)
	}
	if event.CorrelationID != "req-1" {
		t.Fatalf("correlation id = %q, want req-1", event.CorrelationID)
	}

	var payload OrderCreatedPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		t.Fatalf("event payload JSON error = %v", err)
	}
	if payload.OrderID != "ord-1" || payload.TotalCents != 2650 {
		t.Fatalf("payload = %#v", payload)
	}
	if len(payload.Items) != 2 {
		t.Fatalf("payload items = %d, want 2", len(payload.Items))
	}
	if payload.Items[0].MenuItemID != "pizza" ||
		payload.Items[0].Name != "Pizza" ||
		payload.Items[0].UnitPriceCents != 1200 ||
		payload.Items[0].Quantity != 2 ||
		payload.Items[0].LineTotalCents != 2400 {
		t.Fatalf("first payload item = %#v", payload.Items[0])
	}
	if payload.Items[0].ModifierItemIDs == nil {
		t.Fatal("first payload item modifier ids = nil, want empty JSON array")
	}
	if payload.Items[1].MenuItemID != "tea" || payload.Items[1].LineTotalCents != 250 {
		t.Fatalf("second payload item = %#v", payload.Items[1])
	}
}

func TestServiceCreateOrderReturnsStoredOrderWhenIdempotencyKeyIsReused(t *testing.T) {
	existing := mustOrder(t, "existing-order")
	store := &storeStub{
		result: CreateOrderResult{
			Order:                existing,
			ReusedIdempotencyKey: true,
		},
	}
	service := NewService(ServiceConfig{
		Catalog: &catalogStub{
			items: []orderdomain.NewOrderItemInput{
				{MenuItemID: "pizza", Name: "Pizza", UnitPriceCents: 1200, Quantity: 1},
			},
		},
		Store: store,
		NewID: sequenceID("ord-new", "evt-new"),
		Now:   fixedTime,
	})

	result, err := service.CreateOrder(context.Background(), CreateOrderInput{
		RequestID:      "req-1",
		UserID:         "usr-1",
		IdempotencyKey: "idem-1",
		RestaurantID:   "rst-1",
		Items:          []RequestedItem{{MenuItemID: "pizza", Quantity: 1}},
	})
	if err != nil {
		t.Fatalf("CreateOrder() error = %v", err)
	}

	if !result.ReusedIdempotencyKey {
		t.Fatal("ReusedIdempotencyKey = false, want true")
	}
	if result.Order.ID != "existing-order" {
		t.Fatalf("order id = %q, want existing-order", result.Order.ID)
	}
}

func TestServiceCreateOrderRejectsInvalidInputBeforeCatalogCall(t *testing.T) {
	catalog := &catalogStub{}
	service := NewService(ServiceConfig{
		Catalog: catalog,
		Store:   &storeStub{},
		NewID:   sequenceID("ord-1", "evt-1"),
		Now:     fixedTime,
	})

	_, err := service.CreateOrder(context.Background(), CreateOrderInput{
		RequestID:      "req-1",
		UserID:         "usr-1",
		IdempotencyKey: "",
		RestaurantID:   "rst-1",
		Items:          []RequestedItem{{MenuItemID: "pizza", Quantity: 1}},
	})
	if !errors.Is(err, ErrInvalidCreateOrder) {
		t.Fatalf("CreateOrder() error = %v, want %v", err, ErrInvalidCreateOrder)
	}
	if catalog.called {
		t.Fatal("catalog was called for invalid input")
	}
}

func TestServiceCreateOrderPropagatesCatalogErrorWithoutStoreWrite(t *testing.T) {
	store := &storeStub{}
	service := NewService(ServiceConfig{
		Catalog: &catalogStub{err: ErrCatalogUnavailable},
		Store:   store,
		NewID:   sequenceID("ord-1", "evt-1"),
		Now:     fixedTime,
	})

	_, err := service.CreateOrder(context.Background(), CreateOrderInput{
		RequestID:      "req-1",
		UserID:         "usr-1",
		IdempotencyKey: "idem-1",
		RestaurantID:   "rst-1",
		Items:          []RequestedItem{{MenuItemID: "pizza", Quantity: 1}},
	})
	if !errors.Is(err, ErrCatalogUnavailable) {
		t.Fatalf("CreateOrder() error = %v, want %v", err, ErrCatalogUnavailable)
	}
	if store.called {
		t.Fatal("store was called after catalog error")
	}
}

func TestServiceCreateOrderPropagatesLookupErrorWithoutCatalogCall(t *testing.T) {
	wantErr := errors.New("database offline")
	store := &storeStub{lookupErr: wantErr}
	catalog := &catalogStub{}
	service := NewService(ServiceConfig{Catalog: catalog, Store: store})
	_, err := service.CreateOrder(context.Background(), CreateOrderInput{
		UserID: "usr-1", IdempotencyKey: "idem-1", RestaurantID: "rst-1",
		Items: []RequestedItem{{MenuItemID: "pizza", Quantity: 1}},
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if catalog.called || store.called {
		t.Fatal("lookup failure must prevent catalog calls and order writes")
	}
}

func TestCreateOrderRequestFingerprintIgnoresItemOrder(t *testing.T) {
	left := CreateOrderInput{
		UserID:       "usr-1",
		RestaurantID: "rst-1",
		Items: []RequestedItem{
			{MenuItemID: "tea", Quantity: 1},
			{MenuItemID: "pizza", Quantity: 2, ModifierItemIDs: []string{"cheese", "olives"}},
		},
	}
	right := CreateOrderInput{
		UserID:       "usr-1",
		RestaurantID: "rst-1",
		Items: []RequestedItem{
			{MenuItemID: "pizza", Quantity: 2, ModifierItemIDs: []string{"olives", "cheese"}},
			{MenuItemID: "tea", Quantity: 1},
		},
	}

	if FingerprintCreateOrderRequest(left) != FingerprintCreateOrderRequest(right) {
		t.Fatal("equivalent requests produced different fingerprints")
	}
}

func TestServiceGetOrderDelegatesToStoreWithUserScope(t *testing.T) {
	existing := mustOrder(t, "ord-1")
	store := &storeStub{getOrder: existing}
	service := NewService(ServiceConfig{
		Catalog: &catalogStub{},
		Store:   store,
		NewID:   sequenceID("ord-1", "evt-1"),
		Now:     fixedTime,
	})

	order, err := service.GetOrder(context.Background(), GetOrderInput{
		OrderID: "ord-1",
		UserID:  "usr-1",
	})
	if err != nil {
		t.Fatalf("GetOrder() error = %v", err)
	}

	if store.getOrderID != "ord-1" {
		t.Fatalf("store order id = %q, want ord-1", store.getOrderID)
	}
	if store.getUserID != "usr-1" {
		t.Fatalf("store user id = %q, want usr-1", store.getUserID)
	}
	if order.ID != "ord-1" {
		t.Fatalf("order id = %q, want ord-1", order.ID)
	}
}

func TestServiceApplyPartnerEventDelegatesToStore(t *testing.T) {
	store := &storeStub{applyResult: ApplyPartnerEventResult{Order: mustOrder(t, "ord-1"), Changed: true}}
	service := NewService(ServiceConfig{
		Catalog: &catalogStub{},
		Store:   store,
		NewID:   sequenceID("ord-1", "evt-1"),
		Now:     fixedTime,
	})

	result, err := service.ApplyPartnerEvent(context.Background(), ApplyPartnerEventInput{
		EventID:            "evt-partner-1",
		EventType:          EventTypePartnerOrderAccepted,
		OrderID:            "ord-1",
		NewStatus:          orderdomain.StatusAccepted,
		PayloadFingerprint: "payload-fingerprint-1",
	})
	if err != nil {
		t.Fatalf("ApplyPartnerEvent() error = %v", err)
	}

	if store.applyRecord.EventID != "evt-partner-1" {
		t.Fatalf("event id = %q, want evt-partner-1", store.applyRecord.EventID)
	}
	if store.applyRecord.NewStatus != orderdomain.StatusAccepted {
		t.Fatalf("new status = %q, want accepted", store.applyRecord.NewStatus)
	}
	if !result.Changed {
		t.Fatal("changed = false, want true")
	}
}

func TestServiceApplyPartnerEventRejectsInvalidInput(t *testing.T) {
	store := &storeStub{}
	service := NewService(ServiceConfig{
		Catalog: &catalogStub{},
		Store:   store,
		NewID:   sequenceID("ord-1", "evt-1"),
		Now:     fixedTime,
	})

	_, err := service.ApplyPartnerEvent(context.Background(), ApplyPartnerEventInput{
		EventID:            "",
		EventType:          EventTypePartnerOrderAccepted,
		OrderID:            "ord-1",
		NewStatus:          orderdomain.StatusAccepted,
		PayloadFingerprint: "payload-fingerprint-1",
	})
	if !errors.Is(err, ErrInvalidPartnerEvent) {
		t.Fatalf("ApplyPartnerEvent() error = %v, want %v", err, ErrInvalidPartnerEvent)
	}
	if store.applyRecord.EventID != "" {
		t.Fatal("store was called for invalid partner event")
	}
}

type catalogStub struct {
	called         bool
	restaurantID   string
	requestedItems []RequestedItem
	items          []orderdomain.NewOrderItemInput
	err            error
}

func (s *catalogStub) ValidateOrderItems(_ context.Context, restaurantID string, items []RequestedItem) ([]orderdomain.NewOrderItemInput, error) {
	s.called = true
	s.restaurantID = restaurantID
	s.requestedItems = append([]RequestedItem(nil), items...)
	if s.err != nil {
		return nil, s.err
	}
	return append([]orderdomain.NewOrderItemInput(nil), s.items...), nil
}

type storeStub struct {
	lookupErr   error
	called      bool
	record      CreateOrderRecord
	result      CreateOrderResult
	err         error
	getOrderID  string
	getUserID   string
	getOrder    orderdomain.Order
	getErr      error
	applyRecord ApplyPartnerEventRecord
	applyResult ApplyPartnerEventResult
	applyErr    error
}

func (s *storeStub) FindOrderByIdempotencyKey(_ context.Context, _, _, _ string) (CreateOrderResult, bool, error) {
	return CreateOrderResult{}, false, s.lookupErr
}

func (s *storeStub) CreateOrder(_ context.Context, record CreateOrderRecord) (CreateOrderResult, error) {
	s.called = true
	s.record = record
	if s.err != nil {
		return CreateOrderResult{}, s.err
	}
	if s.result.Order.ID != "" {
		return s.result, nil
	}
	return CreateOrderResult{Order: record.Order}, nil
}

func (s *storeStub) GetOrder(_ context.Context, orderID, userID string) (orderdomain.Order, error) {
	s.getOrderID = orderID
	s.getUserID = userID
	if s.getErr != nil {
		return orderdomain.Order{}, s.getErr
	}
	return s.getOrder, nil
}

func (s *storeStub) ApplyPartnerEvent(_ context.Context, record ApplyPartnerEventRecord) (ApplyPartnerEventResult, error) {
	s.applyRecord = record
	if s.applyErr != nil {
		return ApplyPartnerEventResult{}, s.applyErr
	}
	return s.applyResult, nil
}

func sequenceID(values ...string) func() string {
	next := 0
	return func() string {
		value := values[next]
		next++
		return value
	}
}

func fixedTime() time.Time {
	return time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
}

func mustOrder(t *testing.T, id string) orderdomain.Order {
	t.Helper()

	order, err := orderdomain.NewOrder(orderdomain.NewOrderInput{
		OrderID:      id,
		UserID:       "usr-1",
		RestaurantID: "rst-1",
		Items: []orderdomain.NewOrderItemInput{
			{MenuItemID: "pizza", Name: "Pizza", UnitPriceCents: 1200, Quantity: 1},
		},
	})
	if err != nil {
		t.Fatalf("NewOrder() error = %v", err)
	}
	return order
}
