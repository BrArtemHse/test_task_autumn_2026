package kafkahandler

import (
	"context"
	"errors"
	"testing"

	partnerapp "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/application"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/kafkaconsume"
)

func TestOrderCreatedHandlerMapsKafkaEnvelope(t *testing.T) {
	service := &serviceStub{}
	handler, err := NewOrderCreatedHandler(service)
	if err != nil {
		t.Fatalf("NewOrderCreatedHandler() error = %v", err)
	}

	err = handler.Handle(context.Background(), kafkaconsume.Message{
		Topic: "orders.events.v1",
		Key:   []byte("ord-1"),
		Value: []byte(`{"order_id":"ord-1"}`),
		Headers: map[string][]byte{
			"event_id":       []byte("evt-1"),
			"event_type":     []byte("order.created.v1"),
			"event_version":  []byte("1"),
			"correlation_id": []byte("req-1"),
		},
	})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if service.input.EventID != "evt-1" ||
		service.input.EventType != partnerapp.EventTypeOrderCreated ||
		service.input.EventVersion != 1 ||
		service.input.PartitionKey != "ord-1" ||
		service.input.CorrelationID != "req-1" ||
		string(service.input.Payload) != `{"order_id":"ord-1"}` {
		t.Fatalf("mapped input = %#v", service.input)
	}
}

func TestOrderCreatedHandlerRejectsMissingEnvelopeHeaders(t *testing.T) {
	service := &serviceStub{}
	handler, err := NewOrderCreatedHandler(service)
	if err != nil {
		t.Fatalf("NewOrderCreatedHandler() error = %v", err)
	}

	err = handler.Handle(context.Background(), kafkaconsume.Message{
		Key:   []byte("ord-1"),
		Value: []byte(`{}`),
	})
	if !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("Handle() error = %v, want %v", err, ErrInvalidMessage)
	}
	if service.called {
		t.Fatal("service called for invalid Kafka envelope")
	}
}

type serviceStub struct {
	called bool
	input  partnerapp.HandleOrderCreatedInput
	err    error
}

func (s *serviceStub) HandleOrderCreated(_ context.Context, input partnerapp.HandleOrderCreatedInput) (partnerapp.HandleOrderCreatedResult, error) {
	s.called = true
	s.input = input
	return partnerapp.HandleOrderCreatedResult{}, s.err
}
