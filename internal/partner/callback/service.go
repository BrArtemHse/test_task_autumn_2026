package callback

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/contracts/partnerevents"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/events"
)

const (
	EventTypeOrderAccepted = partnerevents.OrderAcceptedV1Type
	EventTypeOrderRejected = partnerevents.OrderRejectedV1Type
)

var (
	ErrInvalidStatusCallback = errors.New("invalid restaurant status callback")
	ErrUnauthorizedCallback  = errors.New("unauthorized restaurant status callback")
	ErrCallbackConflict      = errors.New("callback id reused with different payload")
)

type ServiceConfig struct {
	Store Store
	NewID func() string
	Now   func() time.Time
}

type Service struct {
	store Store
	newID func() string
	now   func() time.Time
}

type Store interface {
	ReceiveStatusCallback(ctx context.Context, record ReceiveRecord) (ReceiveResult, error)
}

type ReceiveStatusCallbackInput struct {
	RequestID         string
	BearerToken       string
	CallbackID        string
	OrderID           string
	RestaurantID      string
	ExternalStoreID   string
	Status            string
	PartnerOccurredAt time.Time
}

type ReceiveRecord struct {
	CallbackID         string
	TokenDigest        string
	PayloadFingerprint string
	Payload            partnerevents.OrderStatusChangedV1
	OutboxEvent        events.Envelope
}

type ReceiveResult struct {
	Duplicate bool
}

func NewService(config ServiceConfig) *Service {
	newID := config.NewID
	if newID == nil {
		newID = func() string { return "" }
	}
	now := config.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Service{
		store: config.Store,
		newID: newID,
		now:   now,
	}
}

func (s *Service) ReceiveStatusCallback(ctx context.Context, input ReceiveStatusCallbackInput) (ReceiveResult, error) {
	eventType, err := validateAndEventType(input)
	if err != nil {
		return ReceiveResult{}, err
	}
	if s.store == nil {
		return ReceiveResult{}, fmt.Errorf("%w: store is required", ErrInvalidStatusCallback)
	}

	payload := partnerevents.OrderStatusChangedV1{
		CallbackID:        input.CallbackID,
		OrderID:           input.OrderID,
		RestaurantID:      input.RestaurantID,
		ExternalStoreID:   input.ExternalStoreID,
		Status:            input.Status,
		PartnerOccurredAt: input.PartnerOccurredAt.UTC(),
	}
	canonicalPayload, err := json.Marshal(payload)
	if err != nil {
		return ReceiveResult{}, err
	}
	payloadFingerprint := sha256.Sum256(canonicalPayload)
	tokenDigest := sha256.Sum256([]byte(input.BearerToken))

	eventID := s.newID()
	if eventID == "" {
		return ReceiveResult{}, fmt.Errorf("%w: event id is required", ErrInvalidStatusCallback)
	}
	event, err := events.NewEnvelope(events.NewEnvelopeInput{
		EventID:       eventID,
		EventType:     eventType,
		EventVersion:  1,
		OccurredAt:    s.now(),
		Producer:      "partner-service",
		AggregateType: "order",
		AggregateID:   payload.OrderID,
		PartitionKey:  payload.OrderID,
		CorrelationID: input.RequestID,
		CausationID:   payload.CallbackID,
		Payload:       canonicalPayload,
	})
	if err != nil {
		return ReceiveResult{}, err
	}

	return s.store.ReceiveStatusCallback(ctx, ReceiveRecord{
		CallbackID:         payload.CallbackID,
		TokenDigest:        hex.EncodeToString(tokenDigest[:]),
		PayloadFingerprint: hex.EncodeToString(payloadFingerprint[:]),
		Payload:            payload,
		OutboxEvent:        event,
	})
}

func validateAndEventType(input ReceiveStatusCallbackInput) (string, error) {
	if input.CallbackID == "" ||
		input.OrderID == "" ||
		input.RestaurantID == "" ||
		input.ExternalStoreID == "" ||
		input.BearerToken == "" ||
		input.PartnerOccurredAt.IsZero() {
		return "", ErrInvalidStatusCallback
	}

	switch input.Status {
	case partnerevents.AcceptedStatus:
		return EventTypeOrderAccepted, nil
	case partnerevents.RejectedStatus:
		return EventTypeOrderRejected, nil
	default:
		return "", ErrInvalidStatusCallback
	}
}
