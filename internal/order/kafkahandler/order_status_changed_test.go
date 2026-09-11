package kafkahandler

import (
	"context"
	"errors"
	"testing"

	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/contracts/partnerevents"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/application"
	orderdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/domain"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/kafkaconsume"
)

func TestPartnerStatusHandlerMapsAcceptedEvent(t *testing.T) {
	service := &partnerEventServiceStub{}
	handler, err := NewPartnerStatusHandler(service)
	if err != nil {
		t.Fatalf("NewPartnerStatusHandler() error = %v", err)
	}

	if err := handler.Handle(context.Background(), acceptedMessage()); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	input := service.inputs[0]
	if input.EventID != "event-1" ||
		input.EventType != partnerevents.OrderAcceptedV1Type ||
		input.OrderID != "order-1" ||
		input.NewStatus != orderdomain.StatusAccepted ||
		input.PayloadFingerprint == "" {
		t.Fatalf("application input = %#v", input)
	}
}

func TestPartnerStatusHandlerMapsRejectedEvent(t *testing.T) {
	service := &partnerEventServiceStub{}
	handler, err := NewPartnerStatusHandler(service)
	if err != nil {
		t.Fatalf("NewPartnerStatusHandler() error = %v", err)
	}
	message := acceptedMessage()
	message.Headers["event_type"] = []byte(partnerevents.OrderRejectedV1Type)
	message.Value = []byte(`{"callback_id":"callback-1","order_id":"order-1","restaurant_id":"restaurant-1","external_store_id":"store-1","status":"rejected","partner_occurred_at":"2026-09-07T12:00:00Z"}`)

	if err := handler.Handle(context.Background(), message); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if service.inputs[0].NewStatus != orderdomain.StatusRejected {
		t.Fatalf("status = %q, want rejected", service.inputs[0].NewStatus)
	}
}

func TestPartnerStatusHandlerCanonicalizesEquivalentTimes(t *testing.T) {
	service := &partnerEventServiceStub{}
	handler, err := NewPartnerStatusHandler(service)
	if err != nil {
		t.Fatalf("NewPartnerStatusHandler() error = %v", err)
	}
	first := acceptedMessage()
	first.Value = []byte(`{"callback_id":"callback-1","order_id":"order-1","restaurant_id":"restaurant-1","external_store_id":"store-1","status":"accepted","partner_occurred_at":"2026-09-07T15:00:00+03:00"}`)
	second := acceptedMessage()

	if err := handler.Handle(context.Background(), first); err != nil {
		t.Fatalf("first Handle() error = %v", err)
	}
	if err := handler.Handle(context.Background(), second); err != nil {
		t.Fatalf("second Handle() error = %v", err)
	}
	if service.inputs[0].PayloadFingerprint != service.inputs[1].PayloadFingerprint {
		t.Fatalf("equivalent payload fingerprints = %q and %q", service.inputs[0].PayloadFingerprint, service.inputs[1].PayloadFingerprint)
	}
}

func TestPartnerStatusHandlerRejectsInvalidEnvelopeOrPayload(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*kafkaconsume.Message)
	}{
		{name: "missing event id", mutate: func(message *kafkaconsume.Message) { delete(message.Headers, "event_id") }},
		{name: "bad version", mutate: func(message *kafkaconsume.Message) { message.Headers["event_version"] = []byte("2") }},
		{name: "bad producer", mutate: func(message *kafkaconsume.Message) { message.Headers["producer"] = []byte("unknown") }},
		{name: "bad aggregate type", mutate: func(message *kafkaconsume.Message) { message.Headers["aggregate_type"] = []byte("restaurant") }},
		{name: "aggregate mismatch", mutate: func(message *kafkaconsume.Message) { message.Headers["aggregate_id"] = []byte("order-2") }},
		{name: "key mismatch", mutate: func(message *kafkaconsume.Message) { message.Key = []byte("order-2") }},
		{name: "status mismatch", mutate: func(message *kafkaconsume.Message) {
			message.Value = []byte(`{"callback_id":"callback-1","order_id":"order-1","restaurant_id":"restaurant-1","external_store_id":"store-1","status":"rejected","partner_occurred_at":"2026-09-07T12:00:00Z"}`)
		}},
		{name: "invalid json", mutate: func(message *kafkaconsume.Message) { message.Value = []byte("{") }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &partnerEventServiceStub{}
			handler, err := NewPartnerStatusHandler(service)
			if err != nil {
				t.Fatalf("NewPartnerStatusHandler() error = %v", err)
			}
			message := acceptedMessage()
			test.mutate(&message)

			err = handler.Handle(context.Background(), message)
			if !errors.Is(err, ErrInvalidPartnerStatusMessage) {
				t.Fatalf("Handle() error = %v, want %v", err, ErrInvalidPartnerStatusMessage)
			}
			if len(service.inputs) != 0 {
				t.Fatal("service called for invalid message")
			}
		})
	}
}

func TestPartnerStatusHandlerPropagatesServiceError(t *testing.T) {
	wantErr := errors.New("database unavailable")
	service := &partnerEventServiceStub{err: wantErr}
	handler, err := NewPartnerStatusHandler(service)
	if err != nil {
		t.Fatalf("NewPartnerStatusHandler() error = %v", err)
	}

	err = handler.Handle(context.Background(), acceptedMessage())
	if !errors.Is(err, wantErr) {
		t.Fatalf("Handle() error = %v, want %v", err, wantErr)
	}
}

type partnerEventServiceStub struct {
	inputs []application.ApplyPartnerEventInput
	err    error
}

func (s *partnerEventServiceStub) ApplyPartnerEvent(_ context.Context, input application.ApplyPartnerEventInput) (application.ApplyPartnerEventResult, error) {
	s.inputs = append(s.inputs, input)
	return application.ApplyPartnerEventResult{}, s.err
}

func acceptedMessage() kafkaconsume.Message {
	return kafkaconsume.Message{
		Topic: "partner.events.v1",
		Key:   []byte("order-1"),
		Value: []byte(`{"callback_id":"callback-1","order_id":"order-1","restaurant_id":"restaurant-1","external_store_id":"store-1","status":"accepted","partner_occurred_at":"2026-09-07T12:00:00Z"}`),
		Headers: map[string][]byte{
			"event_id":       []byte("event-1"),
			"event_type":     []byte(partnerevents.OrderAcceptedV1Type),
			"event_version":  []byte("1"),
			"producer":       []byte("partner-service"),
			"aggregate_type": []byte("order"),
			"aggregate_id":   []byte("order-1"),
		},
	}
}
