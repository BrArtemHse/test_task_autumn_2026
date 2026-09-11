package callback

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestServiceReceivesAcceptedCallback(t *testing.T) {
	store := &storeStub{}
	service := NewService(ServiceConfig{
		Store: store,
		NewID: func() string { return "evt-partner-1" },
		Now:   fixedNow,
	})

	result, err := service.ReceiveStatusCallback(context.Background(), ReceiveStatusCallbackInput{
		RequestID:         "req-1",
		BearerToken:       "development-only-secret",
		CallbackID:        "callback-1",
		OrderID:           "order-1",
		RestaurantID:      "restaurant-1",
		ExternalStoreID:   "store-1",
		Status:            "accepted",
		PartnerOccurredAt: partnerTime(),
	})
	if err != nil {
		t.Fatalf("ReceiveStatusCallback() error = %v", err)
	}
	if result.Duplicate {
		t.Fatal("duplicate = true, want false")
	}

	wantDigest := sha256.Sum256([]byte("development-only-secret"))
	if store.record.TokenDigest != hex.EncodeToString(wantDigest[:]) {
		t.Fatalf("token digest = %q, want %q", store.record.TokenDigest, hex.EncodeToString(wantDigest[:]))
	}
	if store.record.PayloadFingerprint == "" {
		t.Fatal("payload fingerprint is empty")
	}
	if store.record.OutboxEvent.EventID != "evt-partner-1" {
		t.Fatalf("event id = %q, want evt-partner-1", store.record.OutboxEvent.EventID)
	}
	if store.record.OutboxEvent.EventType != EventTypeOrderAccepted {
		t.Fatalf("event type = %q, want %q", store.record.OutboxEvent.EventType, EventTypeOrderAccepted)
	}
	if store.record.OutboxEvent.AggregateID != "order-1" || store.record.OutboxEvent.PartitionKey != "order-1" {
		t.Fatalf("event routing = (%q, %q), want order-1", store.record.OutboxEvent.AggregateID, store.record.OutboxEvent.PartitionKey)
	}
	if store.record.OutboxEvent.CorrelationID != "req-1" || store.record.OutboxEvent.CausationID != "callback-1" {
		t.Fatalf("event correlation = (%q, %q), want (req-1, callback-1)", store.record.OutboxEvent.CorrelationID, store.record.OutboxEvent.CausationID)
	}

	var payload map[string]any
	if err := json.Unmarshal(store.record.OutboxEvent.Payload, &payload); err != nil {
		t.Fatalf("unmarshal event payload: %v", err)
	}
	if payload["status"] != "accepted" || payload["order_id"] != "order-1" {
		t.Fatalf("event payload = %#v", payload)
	}
}

func TestServiceMapsRejectedCallbackEventType(t *testing.T) {
	store := &storeStub{}
	service := NewService(ServiceConfig{
		Store: store,
		NewID: func() string { return "evt-partner-2" },
		Now:   fixedNow,
	})

	_, err := service.ReceiveStatusCallback(context.Background(), validInput("rejected"))
	if err != nil {
		t.Fatalf("ReceiveStatusCallback() error = %v", err)
	}
	if store.record.OutboxEvent.EventType != EventTypeOrderRejected {
		t.Fatalf("event type = %q, want %q", store.record.OutboxEvent.EventType, EventTypeOrderRejected)
	}
}

func TestServiceCanonicalizesEquivalentPartnerTimes(t *testing.T) {
	store := &storeStub{}
	ids := []string{"evt-1", "evt-2"}
	next := 0
	service := NewService(ServiceConfig{
		Store: store,
		NewID: func() string {
			value := ids[next]
			next++
			return value
		},
		Now: fixedNow,
	})

	input := validInput("accepted")
	input.PartnerOccurredAt = time.Date(2026, 9, 7, 15, 0, 0, 0, time.FixedZone("UTC+3", 3*60*60))
	if _, err := service.ReceiveStatusCallback(context.Background(), input); err != nil {
		t.Fatalf("first ReceiveStatusCallback() error = %v", err)
	}
	firstFingerprint := store.record.PayloadFingerprint

	input.PartnerOccurredAt = time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	if _, err := service.ReceiveStatusCallback(context.Background(), input); err != nil {
		t.Fatalf("second ReceiveStatusCallback() error = %v", err)
	}
	if store.record.PayloadFingerprint != firstFingerprint {
		t.Fatalf("equivalent timestamps produced fingerprints %q and %q", firstFingerprint, store.record.PayloadFingerprint)
	}
}

func TestServiceReturnsStoreResult(t *testing.T) {
	store := &storeStub{result: ReceiveResult{Duplicate: true}}
	service := NewService(ServiceConfig{
		Store: store,
		NewID: func() string { return "evt-partner-1" },
		Now:   fixedNow,
	})

	result, err := service.ReceiveStatusCallback(context.Background(), validInput("accepted"))
	if err != nil {
		t.Fatalf("ReceiveStatusCallback() error = %v", err)
	}
	if !result.Duplicate {
		t.Fatal("duplicate = false, want true")
	}
}

func TestServiceRejectsInvalidCallback(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ReceiveStatusCallbackInput)
	}{
		{name: "missing callback id", mutate: func(input *ReceiveStatusCallbackInput) { input.CallbackID = "" }},
		{name: "missing order id", mutate: func(input *ReceiveStatusCallbackInput) { input.OrderID = "" }},
		{name: "missing restaurant id", mutate: func(input *ReceiveStatusCallbackInput) { input.RestaurantID = "" }},
		{name: "missing external store id", mutate: func(input *ReceiveStatusCallbackInput) { input.ExternalStoreID = "" }},
		{name: "missing token", mutate: func(input *ReceiveStatusCallbackInput) { input.BearerToken = "" }},
		{name: "missing partner time", mutate: func(input *ReceiveStatusCallbackInput) { input.PartnerOccurredAt = time.Time{} }},
		{name: "unsupported status", mutate: func(input *ReceiveStatusCallbackInput) { input.Status = "preparing" }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &storeStub{}
			service := NewService(ServiceConfig{
				Store: store,
				NewID: func() string { return "evt-partner-1" },
				Now:   fixedNow,
			})
			input := validInput("accepted")
			test.mutate(&input)

			_, err := service.ReceiveStatusCallback(context.Background(), input)
			if !errors.Is(err, ErrInvalidStatusCallback) {
				t.Fatalf("ReceiveStatusCallback() error = %v, want %v", err, ErrInvalidStatusCallback)
			}
			if store.called {
				t.Fatal("store called for invalid callback")
			}
		})
	}
}

func TestServiceRejectsMissingGeneratedEventID(t *testing.T) {
	store := &storeStub{}
	service := NewService(ServiceConfig{
		Store: store,
		NewID: func() string { return "" },
		Now:   fixedNow,
	})

	_, err := service.ReceiveStatusCallback(context.Background(), validInput("accepted"))
	if !errors.Is(err, ErrInvalidStatusCallback) {
		t.Fatalf("ReceiveStatusCallback() error = %v, want %v", err, ErrInvalidStatusCallback)
	}
	if store.called {
		t.Fatal("store called with empty event id")
	}
}

type storeStub struct {
	called bool
	record ReceiveRecord
	result ReceiveResult
	err    error
}

func (s *storeStub) ReceiveStatusCallback(_ context.Context, record ReceiveRecord) (ReceiveResult, error) {
	s.called = true
	s.record = record
	return s.result, s.err
}

func validInput(status string) ReceiveStatusCallbackInput {
	return ReceiveStatusCallbackInput{
		RequestID:         "req-1",
		BearerToken:       "development-only-secret",
		CallbackID:        "callback-1",
		OrderID:           "order-1",
		RestaurantID:      "restaurant-1",
		ExternalStoreID:   "store-1",
		Status:            status,
		PartnerOccurredAt: partnerTime(),
	}
}

func fixedNow() time.Time {
	return time.Date(2026, 9, 7, 12, 1, 0, 0, time.UTC)
}

func partnerTime() time.Time {
	return time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
}
