package kafkapub

import (
	"errors"
	"reflect"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/outbox"
)

func TestNewPublisherRejectsMissingBrokers(t *testing.T) {
	_, err := NewPublisher([]string{"", "   "})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("NewPublisher() error = %v, want %v", err, ErrInvalidConfig)
	}
}

func TestBuildKafkaMessagePreservesIdentityKeyPayloadAndHeaders(t *testing.T) {
	message := outbox.Message{
		EventID: "evt-1",
		Topic:   "orders.events.v1",
		Key:     "ord-1",
		Value:   []byte(`{"order_id":"ord-1"}`),
		Headers: map[string]string{
			"producer":      "order-service",
			"event_type":    "order.created.v1",
			"event_version": "1",
		},
	}

	got := buildKafkaMessage(message)
	want := kafka.Message{
		Topic: "orders.events.v1",
		Key:   []byte("ord-1"),
		Value: []byte(`{"order_id":"ord-1"}`),
		Headers: []kafka.Header{
			{Key: "event_id", Value: []byte("evt-1")},
			{Key: "event_type", Value: []byte("order.created.v1")},
			{Key: "event_version", Value: []byte("1")},
			{Key: "producer", Value: []byte("order-service")},
		},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("kafka message = %#v, want %#v", got, want)
	}
}
