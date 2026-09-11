package kafkahandler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/contracts/partnerevents"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/application"
	orderdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/domain"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/kafkaconsume"
)

var ErrInvalidPartnerStatusMessage = errors.New("invalid Kafka partner status event")

type PartnerEventService interface {
	ApplyPartnerEvent(context.Context, application.ApplyPartnerEventInput) (application.ApplyPartnerEventResult, error)
}

type PartnerStatusHandler struct {
	service PartnerEventService
}

func NewPartnerStatusHandler(service PartnerEventService) (*PartnerStatusHandler, error) {
	if service == nil {
		return nil, fmt.Errorf("%w: service is required", ErrInvalidPartnerStatusMessage)
	}
	return &PartnerStatusHandler{service: service}, nil
}

func (h *PartnerStatusHandler) Handle(ctx context.Context, message kafkaconsume.Message) error {
	eventID := string(message.Headers["event_id"])
	eventType := string(message.Headers["event_type"])
	eventVersion, err := strconv.Atoi(string(message.Headers["event_version"]))
	if err != nil {
		return fmt.Errorf("%w: event version", ErrInvalidPartnerStatusMessage)
	}
	if eventID == "" ||
		eventVersion != 1 ||
		string(message.Headers["producer"]) != "partner-service" ||
		string(message.Headers["aggregate_type"]) != "order" ||
		len(message.Key) == 0 ||
		len(message.Value) == 0 {
		return ErrInvalidPartnerStatusMessage
	}

	var payload partnerevents.OrderStatusChangedV1
	if err := json.Unmarshal(message.Value, &payload); err != nil {
		return fmt.Errorf("%w: payload json", ErrInvalidPartnerStatusMessage)
	}
	status, err := validatePartnerStatusPayload(eventType, string(message.Key), string(message.Headers["aggregate_id"]), payload)
	if err != nil {
		return err
	}
	payload.PartnerOccurredAt = payload.PartnerOccurredAt.UTC()
	canonicalPayload, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	fingerprint := sha256.Sum256(canonicalPayload)

	_, err = h.service.ApplyPartnerEvent(ctx, application.ApplyPartnerEventInput{
		EventID:            eventID,
		EventType:          eventType,
		OrderID:            payload.OrderID,
		NewStatus:          status,
		PayloadFingerprint: hex.EncodeToString(fingerprint[:]),
	})
	return err
}

func validatePartnerStatusPayload(eventType, partitionKey, aggregateID string, payload partnerevents.OrderStatusChangedV1) (orderdomain.Status, error) {
	if payload.CallbackID == "" ||
		payload.OrderID == "" ||
		payload.RestaurantID == "" ||
		payload.ExternalStoreID == "" ||
		payload.PartnerOccurredAt.IsZero() ||
		payload.OrderID != partitionKey ||
		payload.OrderID != aggregateID {
		return "", ErrInvalidPartnerStatusMessage
	}

	switch {
	case eventType == partnerevents.OrderAcceptedV1Type && payload.Status == partnerevents.AcceptedStatus:
		return orderdomain.StatusAccepted, nil
	case eventType == partnerevents.OrderRejectedV1Type && payload.Status == partnerevents.RejectedStatus:
		return orderdomain.StatusRejected, nil
	default:
		return "", ErrInvalidPartnerStatusMessage
	}
}
