package application

import (
	"context"
	"errors"
	"testing"
)

func TestServiceHandleOrderCreatedStoresValidatedSubmission(t *testing.T) {
	store := &storeStub{}
	service := NewService(ServiceConfig{Store: store, NewID: func() string { return "sub-1" }})

	result, err := service.HandleOrderCreated(context.Background(), HandleOrderCreatedInput{
		EventID:       "evt-1",
		EventType:     EventTypeOrderCreated,
		EventVersion:  1,
		PartitionKey:  "ord-1",
		CorrelationID: "req-1",
		Payload: []byte(`{
			"order_id":"ord-1",
			"user_id":"usr-1",
			"restaurant_id":"rst-pizza-1",
			"status":"pending_restaurant_confirmation",
			"total_cents":138000,
			"items":[{
				"menu_item_id":"item-margherita",
				"name":"Margherita",
				"unit_price_cents":69000,
				"quantity":2,
				"modifier_item_ids":["mod-extra-cheese"],
				"line_total_cents":138000
			}]
		}`),
	})
	if err != nil {
		t.Fatalf("HandleOrderCreated() error = %v", err)
	}
	if result.SubmissionID != "sub-1" || result.Duplicate {
		t.Fatalf("result = %#v", result)
	}
	if store.record.EventID != "evt-1" || store.record.OrderID != "ord-1" {
		t.Fatalf("stored identity = %#v", store.record)
	}
	if store.record.RestaurantID != "rst-pizza-1" {
		t.Fatalf("restaurant id = %q", store.record.RestaurantID)
	}
	if store.record.PayloadFingerprint == "" {
		t.Fatal("payload fingerprint is empty")
	}
	if len(store.record.Payload.Items) != 1 || store.record.Payload.Items[0].Quantity != 2 {
		t.Fatalf("stored payload = %#v", store.record.Payload)
	}
}

func TestServiceHandleOrderCreatedUsesSemanticPayloadFingerprint(t *testing.T) {
	store := &storeStub{}
	service := NewService(ServiceConfig{Store: store, NewID: func() string { return "sub-1" }})
	compact := []byte(`{"order_id":"ord-1","user_id":"usr-1","restaurant_id":"rst-pizza-1","status":"pending_restaurant_confirmation","total_cents":69000,"items":[{"menu_item_id":"item-margherita","name":"Margherita","unit_price_cents":69000,"quantity":1,"modifier_item_ids":[],"line_total_cents":69000}]}`)
	formatted := []byte(`{
		"items": [{"quantity": 1, "name": "Margherita", "menu_item_id": "item-margherita", "line_total_cents": 69000, "modifier_item_ids": [], "unit_price_cents": 69000}],
		"total_cents": 69000,
		"status": "pending_restaurant_confirmation",
		"restaurant_id": "rst-pizza-1",
		"user_id": "usr-1",
		"order_id": "ord-1"
	}`)

	first := validInput(compact)
	if _, err := service.HandleOrderCreated(context.Background(), first); err != nil {
		t.Fatalf("first HandleOrderCreated() error = %v", err)
	}
	firstFingerprint := store.record.PayloadFingerprint

	second := validInput(formatted)
	if _, err := service.HandleOrderCreated(context.Background(), second); err != nil {
		t.Fatalf("second HandleOrderCreated() error = %v", err)
	}
	if store.record.PayloadFingerprint != firstFingerprint {
		t.Fatalf("semantic fingerprints differ: %q != %q", store.record.PayloadFingerprint, firstFingerprint)
	}

	withNullModifiers := []byte(`{"order_id":"ord-1","user_id":"usr-1","restaurant_id":"rst-pizza-1","status":"pending_restaurant_confirmation","total_cents":69000,"items":[{"menu_item_id":"item-margherita","name":"Margherita","unit_price_cents":69000,"quantity":1,"modifier_item_ids":null,"line_total_cents":69000}]}`)
	if _, err := service.HandleOrderCreated(context.Background(), validInput(withNullModifiers)); err != nil {
		t.Fatalf("null modifiers HandleOrderCreated() error = %v", err)
	}
	if store.record.PayloadFingerprint != firstFingerprint {
		t.Fatalf("null and empty modifiers fingerprints differ: %q != %q", store.record.PayloadFingerprint, firstFingerprint)
	}
}

func TestServiceHandleOrderCreatedRejectsInvalidEnvelopeAndPayload(t *testing.T) {
	store := &storeStub{}
	service := NewService(ServiceConfig{Store: store, NewID: func() string { return "sub-1" }})

	tests := []struct {
		name  string
		input HandleOrderCreatedInput
	}{
		{name: "unsupported version", input: withVersion(validInput(validPayload()), 2)},
		{name: "key mismatch", input: withKey(validInput(validPayload()), "other-order")},
		{name: "invalid total", input: validInput([]byte(`{"order_id":"ord-1","user_id":"usr-1","restaurant_id":"rst-pizza-1","status":"pending_restaurant_confirmation","total_cents":1,"items":[{"menu_item_id":"item-margherita","name":"Margherita","unit_price_cents":69000,"quantity":1,"modifier_item_ids":[],"line_total_cents":69000}]}`))},
		{name: "malformed json", input: validInput([]byte(`{"order_id":`))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store.called = false
			_, err := service.HandleOrderCreated(context.Background(), tt.input)
			if !errors.Is(err, ErrInvalidOrderCreated) {
				t.Fatalf("HandleOrderCreated() error = %v, want %v", err, ErrInvalidOrderCreated)
			}
			if store.called {
				t.Fatal("store called for invalid event")
			}
		})
	}
}

func validInput(payload []byte) HandleOrderCreatedInput {
	return HandleOrderCreatedInput{
		EventID:      "evt-1",
		EventType:    EventTypeOrderCreated,
		EventVersion: 1,
		PartitionKey: "ord-1",
		Payload:      payload,
	}
}

func validPayload() []byte {
	return []byte(`{"order_id":"ord-1","user_id":"usr-1","restaurant_id":"rst-pizza-1","status":"pending_restaurant_confirmation","total_cents":69000,"items":[{"menu_item_id":"item-margherita","name":"Margherita","unit_price_cents":69000,"quantity":1,"modifier_item_ids":[],"line_total_cents":69000}]}`)
}

func withVersion(input HandleOrderCreatedInput, version int) HandleOrderCreatedInput {
	input.EventVersion = version
	return input
}

func withKey(input HandleOrderCreatedInput, key string) HandleOrderCreatedInput {
	input.PartitionKey = key
	return input
}

type storeStub struct {
	called bool
	record ReceiveOrderRecord
	result ReceiveOrderResult
	err    error
}

func (s *storeStub) ReceiveOrder(_ context.Context, record ReceiveOrderRecord) (ReceiveOrderResult, error) {
	s.called = true
	s.record = record
	return s.result, s.err
}
