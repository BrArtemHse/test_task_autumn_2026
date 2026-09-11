package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/contracts/orderevents"
)

const EventTypeOrderCreated = orderevents.OrderCreatedV1Type

var (
	ErrInvalidOrderCreated = errors.New("invalid order.created.v1 event")
	ErrInboxConflict       = errors.New("inbox event id reused with different payload")
	ErrIntegrationNotFound = errors.New("active restaurant integration not found")
)

type ServiceConfig struct {
	Store Store
	NewID func() string
}

type Service struct {
	store Store
	newID func() string
}

type Store interface {
	ReceiveOrder(ctx context.Context, record ReceiveOrderRecord) (ReceiveOrderResult, error)
}

type HandleOrderCreatedInput struct {
	EventID       string
	EventType     string
	EventVersion  int
	PartitionKey  string
	CorrelationID string
	Payload       []byte
}

type ReceiveOrderRecord struct {
	SubmissionID       string
	EventID            string
	EventType          string
	EventVersion       int
	CorrelationID      string
	OrderID            string
	RestaurantID       string
	PayloadFingerprint string
	Payload            orderevents.OrderCreatedV1
}

type ReceiveOrderResult struct {
	SubmissionID string
	Duplicate    bool
}

type HandleOrderCreatedResult = ReceiveOrderResult

func NewService(config ServiceConfig) *Service {
	newID := config.NewID
	if newID == nil {
		newID = func() string { return "" }
	}
	return &Service{store: config.Store, newID: newID}
}

func (s *Service) HandleOrderCreated(ctx context.Context, input HandleOrderCreatedInput) (HandleOrderCreatedResult, error) {
	if input.EventID == "" ||
		input.EventType != EventTypeOrderCreated ||
		input.EventVersion != 1 ||
		input.PartitionKey == "" ||
		len(input.Payload) == 0 {
		return HandleOrderCreatedResult{}, ErrInvalidOrderCreated
	}
	if s.store == nil {
		return HandleOrderCreatedResult{}, fmt.Errorf("%w: store is required", ErrInvalidOrderCreated)
	}

	var payload orderevents.OrderCreatedV1
	if err := json.Unmarshal(input.Payload, &payload); err != nil {
		return HandleOrderCreatedResult{}, fmt.Errorf("%w: payload json: %v", ErrInvalidOrderCreated, err)
	}
	if err := validatePayload(payload, input.PartitionKey); err != nil {
		return HandleOrderCreatedResult{}, err
	}
	normalizePayload(&payload)

	canonicalPayload, err := json.Marshal(payload)
	if err != nil {
		return HandleOrderCreatedResult{}, err
	}
	fingerprint := sha256.Sum256(canonicalPayload)
	submissionID := s.newID()
	if submissionID == "" {
		return HandleOrderCreatedResult{}, fmt.Errorf("%w: submission id is required", ErrInvalidOrderCreated)
	}

	result, err := s.store.ReceiveOrder(ctx, ReceiveOrderRecord{
		SubmissionID:       submissionID,
		EventID:            input.EventID,
		EventType:          input.EventType,
		EventVersion:       input.EventVersion,
		CorrelationID:      input.CorrelationID,
		OrderID:            payload.OrderID,
		RestaurantID:       payload.RestaurantID,
		PayloadFingerprint: hex.EncodeToString(fingerprint[:]),
		Payload:            payload,
	})
	if err != nil {
		return HandleOrderCreatedResult{}, err
	}
	if result.SubmissionID == "" {
		result.SubmissionID = submissionID
	}
	return result, nil
}

func normalizePayload(payload *orderevents.OrderCreatedV1) {
	for index := range payload.Items {
		if payload.Items[index].ModifierItemIDs == nil {
			payload.Items[index].ModifierItemIDs = []string{}
		}
	}
}

func validatePayload(payload orderevents.OrderCreatedV1, partitionKey string) error {
	if payload.OrderID == "" ||
		payload.OrderID != partitionKey ||
		payload.UserID == "" ||
		payload.RestaurantID == "" ||
		payload.Status != orderevents.PendingStatus ||
		payload.TotalCents <= 0 ||
		len(payload.Items) == 0 {
		return ErrInvalidOrderCreated
	}

	var total int64
	for _, item := range payload.Items {
		if item.MenuItemID == "" ||
			item.Name == "" ||
			item.UnitPriceCents <= 0 ||
			item.Quantity <= 0 ||
			item.LineTotalCents != item.UnitPriceCents*int64(item.Quantity) {
			return ErrInvalidOrderCreated
		}
		total += item.LineTotalCents
	}
	if total != payload.TotalCents {
		return ErrInvalidOrderCreated
	}
	return nil
}
