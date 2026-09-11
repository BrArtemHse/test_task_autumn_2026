package events

import (
	"errors"
	"testing"
	"time"
)

func TestNewEnvelopeRequiresIdentityAndRoutingFields(t *testing.T) {
	tests := []struct {
		name  string
		input NewEnvelopeInput
	}{
		{name: "missing event id", input: validEnvelopeInput(func(in *NewEnvelopeInput) { in.EventID = "" })},
		{name: "missing event type", input: validEnvelopeInput(func(in *NewEnvelopeInput) { in.EventType = "" })},
		{name: "missing producer", input: validEnvelopeInput(func(in *NewEnvelopeInput) { in.Producer = "" })},
		{name: "missing aggregate type", input: validEnvelopeInput(func(in *NewEnvelopeInput) { in.AggregateType = "" })},
		{name: "missing aggregate id", input: validEnvelopeInput(func(in *NewEnvelopeInput) { in.AggregateID = "" })},
		{name: "missing partition key", input: validEnvelopeInput(func(in *NewEnvelopeInput) { in.PartitionKey = "" })},
		{name: "missing payload", input: validEnvelopeInput(func(in *NewEnvelopeInput) { in.Payload = nil })},
		{name: "invalid version", input: validEnvelopeInput(func(in *NewEnvelopeInput) { in.EventVersion = 0 })},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewEnvelope(tt.input)
			if !errors.Is(err, ErrInvalidEnvelope) {
				t.Fatalf("NewEnvelope() error = %v, want %v", err, ErrInvalidEnvelope)
			}
		})
	}
}

func TestNewEnvelopeBuildsEventMetadata(t *testing.T) {
	occurredAt := time.Date(2026, 9, 6, 10, 30, 0, 0, time.UTC)

	envelope, err := NewEnvelope(validEnvelopeInput(func(in *NewEnvelopeInput) {
		in.OccurredAt = occurredAt
	}))
	if err != nil {
		t.Fatalf("NewEnvelope() error = %v", err)
	}

	if envelope.EventID != "evt-1" {
		t.Fatalf("event id = %q, want evt-1", envelope.EventID)
	}
	if envelope.EventVersion != 1 {
		t.Fatalf("event version = %d, want 1", envelope.EventVersion)
	}
	if !envelope.OccurredAt.Equal(occurredAt) {
		t.Fatalf("occurred at = %s, want %s", envelope.OccurredAt, occurredAt)
	}
}

func TestNewEnvelopeCopiesPayload(t *testing.T) {
	payload := []byte(`{"order_id":"ord-1"}`)

	envelope, err := NewEnvelope(validEnvelopeInput(func(in *NewEnvelopeInput) {
		in.Payload = payload
	}))
	if err != nil {
		t.Fatalf("NewEnvelope() error = %v", err)
	}

	payload[0] = '['

	if string(envelope.Payload) != `{"order_id":"ord-1"}` {
		t.Fatalf("payload = %s, want original JSON object", envelope.Payload)
	}
}

func validEnvelopeInput(mutators ...func(*NewEnvelopeInput)) NewEnvelopeInput {
	input := NewEnvelopeInput{
		EventID:       "evt-1",
		EventType:     "order.created",
		EventVersion:  1,
		OccurredAt:    time.Date(2026, 9, 6, 10, 30, 0, 0, time.UTC),
		Producer:      "order-service",
		AggregateType: "order",
		AggregateID:   "ord-1",
		PartitionKey:  "ord-1",
		CorrelationID: "corr-1",
		CausationID:   "cmd-1",
		Payload:       []byte(`{"order_id":"ord-1"}`),
	}

	for _, mutate := range mutators {
		mutate(&input)
	}

	return input
}
