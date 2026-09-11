package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/contracts/orderevents"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/contracts/partnerevents"
	orderdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/domain"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/events"
)

const (
	EventTypeOrderCreated         = orderevents.OrderCreatedV1Type
	EventTypePartnerOrderAccepted = partnerevents.OrderAcceptedV1Type
	EventTypePartnerOrderRejected = partnerevents.OrderRejectedV1Type
	EventTypePartnerStatusChanged = "partner.order_status_changed.v1"
)

var (
	ErrInvalidCreateOrder  = errors.New("invalid create order command")
	ErrCatalogUnavailable  = errors.New("catalog unavailable")
	ErrIdempotencyConflict = errors.New("idempotency key reused with different payload")
	ErrOrderNotFound       = errors.New("order not found")
	ErrInvalidPartnerEvent = errors.New("invalid partner event")
	ErrInboxConflict       = errors.New("inbox event id reused with different payload")
)

type ServiceConfig struct {
	Catalog Catalog
	Store   OrderStore
	NewID   func() string
	Now     func() time.Time
}

type Service struct {
	catalog Catalog
	store   OrderStore
	newID   func() string
	now     func() time.Time
}

type Catalog interface {
	ValidateOrderItems(ctx context.Context, restaurantID string, items []RequestedItem) ([]orderdomain.NewOrderItemInput, error)
}

type OrderStore interface {
	FindOrderByIdempotencyKey(ctx context.Context, userID, idempotencyKey, fingerprint string) (CreateOrderResult, bool, error)
	CreateOrder(ctx context.Context, record CreateOrderRecord) (CreateOrderResult, error)
	GetOrder(ctx context.Context, orderID, userID string) (orderdomain.Order, error)
	ApplyPartnerEvent(ctx context.Context, record ApplyPartnerEventRecord) (ApplyPartnerEventResult, error)
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
		catalog: config.Catalog,
		store:   config.Store,
		newID:   newID,
		now:     now,
	}
}

type CreateOrderInput struct {
	RequestID      string
	UserID         string
	IdempotencyKey string
	RestaurantID   string
	Items          []RequestedItem
}

type RequestedItem struct {
	MenuItemID      string
	Quantity        int32
	ModifierItemIDs []string
}

type GetOrderInput struct {
	OrderID string
	UserID  string
}

type ApplyPartnerEventInput struct {
	EventID            string
	EventType          string
	OrderID            string
	NewStatus          orderdomain.Status
	PayloadFingerprint string
}

type CreateOrderRecord struct {
	Order              orderdomain.Order
	IdempotencyKey     string
	RequestFingerprint string
	OutboxEvent        events.Envelope
}

type CreateOrderResult struct {
	Order                orderdomain.Order
	ReusedIdempotencyKey bool
}

type ApplyPartnerEventRecord struct {
	EventID            string
	EventType          string
	OrderID            string
	NewStatus          orderdomain.Status
	PayloadFingerprint string
}

type ApplyPartnerEventResult struct {
	Order     orderdomain.Order
	Duplicate bool
	Changed   bool
}

type OrderCreatedPayload = orderevents.OrderCreatedV1

type OrderCreatedItemPayload = orderevents.OrderCreatedItemV1

func (s *Service) CreateOrder(ctx context.Context, input CreateOrderInput) (CreateOrderResult, error) {
	if err := validateCreateOrderInput(input); err != nil {
		return CreateOrderResult{}, err
	}
	if s.store == nil {
		return CreateOrderResult{}, fmt.Errorf("%w: store port is required", ErrInvalidCreateOrder)
	}

	fingerprint := FingerprintCreateOrderRequest(input)
	existing, found, err := s.store.FindOrderByIdempotencyKey(ctx, input.UserID, input.IdempotencyKey, fingerprint)
	if err != nil {
		return CreateOrderResult{}, err
	}
	if found {
		return existing, nil
	}
	if s.catalog == nil {
		return CreateOrderResult{}, fmt.Errorf("%w: catalog port is required", ErrInvalidCreateOrder)
	}

	validatedItems, err := s.catalog.ValidateOrderItems(ctx, input.RestaurantID, copyRequestedItems(input.Items))
	if err != nil {
		return CreateOrderResult{}, err
	}

	order, err := orderdomain.NewOrder(orderdomain.NewOrderInput{
		OrderID:      s.newID(),
		UserID:       input.UserID,
		RestaurantID: input.RestaurantID,
		Items:        validatedItems,
	})
	if err != nil {
		return CreateOrderResult{}, err
	}

	payload, err := json.Marshal(OrderCreatedPayload{
		OrderID:      order.ID,
		UserID:       order.UserID,
		RestaurantID: order.RestaurantID,
		Status:       string(order.Status),
		TotalCents:   order.TotalCents,
		Items:        orderCreatedItems(order.Items),
	})
	if err != nil {
		return CreateOrderResult{}, err
	}

	event, err := events.NewEnvelope(events.NewEnvelopeInput{
		EventID:       s.newID(),
		EventType:     EventTypeOrderCreated,
		EventVersion:  1,
		OccurredAt:    s.now(),
		Producer:      "order-service",
		AggregateType: "order",
		AggregateID:   order.ID,
		PartitionKey:  order.ID,
		CorrelationID: input.RequestID,
		CausationID:   input.IdempotencyKey,
		Payload:       payload,
	})
	if err != nil {
		return CreateOrderResult{}, err
	}

	return s.store.CreateOrder(ctx, CreateOrderRecord{
		Order:              order,
		IdempotencyKey:     input.IdempotencyKey,
		RequestFingerprint: fingerprint,
		OutboxEvent:        event,
	})
}

func orderCreatedItems(items []orderdomain.OrderItem) []OrderCreatedItemPayload {
	payloadItems := make([]OrderCreatedItemPayload, 0, len(items))
	for _, item := range items {
		payloadItems = append(payloadItems, OrderCreatedItemPayload{
			MenuItemID:      item.MenuItemID,
			Name:            item.Name,
			UnitPriceCents:  item.UnitPriceCents,
			Quantity:        item.Quantity,
			ModifierItemIDs: append([]string{}, item.ModifierItemIDs...),
			LineTotalCents:  item.LineTotalCents,
		})
	}
	return payloadItems
}

func (s *Service) GetOrder(ctx context.Context, input GetOrderInput) (orderdomain.Order, error) {
	if input.OrderID == "" || input.UserID == "" {
		return orderdomain.Order{}, ErrOrderNotFound
	}
	if s.store == nil {
		return orderdomain.Order{}, fmt.Errorf("%w: store port is required", ErrInvalidCreateOrder)
	}
	return s.store.GetOrder(ctx, input.OrderID, input.UserID)
}

func (s *Service) ApplyPartnerEvent(ctx context.Context, input ApplyPartnerEventInput) (ApplyPartnerEventResult, error) {
	if input.EventID == "" ||
		input.EventType == "" ||
		input.OrderID == "" ||
		input.NewStatus == "" ||
		input.PayloadFingerprint == "" {
		return ApplyPartnerEventResult{}, ErrInvalidPartnerEvent
	}
	if s.store == nil {
		return ApplyPartnerEventResult{}, fmt.Errorf("%w: store port is required", ErrInvalidCreateOrder)
	}

	return s.store.ApplyPartnerEvent(ctx, ApplyPartnerEventRecord(input))
}

func validateCreateOrderInput(input CreateOrderInput) error {
	if input.UserID == "" || input.IdempotencyKey == "" || input.RestaurantID == "" || len(input.Items) == 0 {
		return ErrInvalidCreateOrder
	}
	for i, item := range input.Items {
		if item.MenuItemID == "" || item.Quantity <= 0 {
			return fmt.Errorf("%w: item %d", ErrInvalidCreateOrder, i)
		}
	}
	return nil
}

func copyRequestedItems(items []RequestedItem) []RequestedItem {
	copied := make([]RequestedItem, 0, len(items))
	for _, item := range items {
		item.ModifierItemIDs = append([]string(nil), item.ModifierItemIDs...)
		copied = append(copied, item)
	}
	return copied
}
