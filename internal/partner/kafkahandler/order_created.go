package kafkahandler

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	partnerapp "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/application"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/kafkaconsume"
)

var ErrInvalidMessage = errors.New("invalid Kafka order event")

type OrderCreatedService interface {
	HandleOrderCreated(ctx context.Context, input partnerapp.HandleOrderCreatedInput) (partnerapp.HandleOrderCreatedResult, error)
}

type OrderCreatedHandler struct {
	service OrderCreatedService
}

func NewOrderCreatedHandler(service OrderCreatedService) (*OrderCreatedHandler, error) {
	if service == nil {
		return nil, fmt.Errorf("%w: service is required", ErrInvalidMessage)
	}
	return &OrderCreatedHandler{service: service}, nil
}

func (h *OrderCreatedHandler) Handle(ctx context.Context, message kafkaconsume.Message) error {
	eventID := string(message.Headers["event_id"])
	eventType := string(message.Headers["event_type"])
	rawVersion := string(message.Headers["event_version"])
	if eventID == "" || eventType == "" || rawVersion == "" || len(message.Key) == 0 || len(message.Value) == 0 {
		return ErrInvalidMessage
	}
	eventVersion, err := strconv.Atoi(rawVersion)
	if err != nil {
		return fmt.Errorf("%w: event version: %v", ErrInvalidMessage, err)
	}

	_, err = h.service.HandleOrderCreated(ctx, partnerapp.HandleOrderCreatedInput{
		EventID:       eventID,
		EventType:     eventType,
		EventVersion:  eventVersion,
		PartitionKey:  string(message.Key),
		CorrelationID: string(message.Headers["correlation_id"]),
		Payload:       append([]byte(nil), message.Value...),
	})
	return err
}
