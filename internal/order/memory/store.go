package memory

import (
	"context"
	"sync"

	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/application"
	orderdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/domain"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/events"
)

type Store struct {
	mu          sync.Mutex
	orders      map[string]orderdomain.Order
	idempotency map[idempotencyKey]idempotencyRecord
	inbox       map[string]inboxRecord
	outbox      []events.Envelope
}

type idempotencyKey struct {
	userID string
	key    string
}

type idempotencyRecord struct {
	fingerprint string
	orderID     string
}

type inboxRecord struct {
	fingerprint string
	orderID     string
}

func NewStore() *Store {
	return &Store{
		orders:      make(map[string]orderdomain.Order),
		idempotency: make(map[idempotencyKey]idempotencyRecord),
		inbox:       make(map[string]inboxRecord),
	}
}

func (s *Store) FindOrderByIdempotencyKey(_ context.Context, userID, key, fingerprint string) (application.CreateOrderResult, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.findOrderByIdempotencyKey(idempotencyKey{userID: userID, key: key}, fingerprint)
}

func (s *Store) findOrderByIdempotencyKey(key idempotencyKey, fingerprint string) (application.CreateOrderResult, bool, error) {
	if existing, ok := s.idempotency[key]; ok {
		if existing.fingerprint != fingerprint {
			return application.CreateOrderResult{}, true, application.ErrIdempotencyConflict
		}
		return application.CreateOrderResult{
			Order:                copyOrder(s.orders[existing.orderID]),
			ReusedIdempotencyKey: true,
		}, true, nil
	}
	return application.CreateOrderResult{}, false, nil
}

func (s *Store) CreateOrder(_ context.Context, record application.CreateOrderRecord) (application.CreateOrderResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := idempotencyKey{userID: record.Order.UserID, key: record.IdempotencyKey}
	if existing, found, err := s.findOrderByIdempotencyKey(key, record.RequestFingerprint); err != nil || found {
		return existing, err
	}

	s.orders[record.Order.ID] = copyOrder(record.Order)
	s.idempotency[key] = idempotencyRecord{
		fingerprint: record.RequestFingerprint,
		orderID:     record.Order.ID,
	}
	s.outbox = append(s.outbox, copyEnvelope(record.OutboxEvent))

	return application.CreateOrderResult{Order: copyOrder(record.Order)}, nil
}

func (s *Store) GetOrder(_ context.Context, orderID, userID string) (orderdomain.Order, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	order, ok := s.orders[orderID]
	if !ok || order.UserID != userID {
		return orderdomain.Order{}, application.ErrOrderNotFound
	}

	return copyOrder(order), nil
}

func (s *Store) ApplyPartnerEvent(_ context.Context, record application.ApplyPartnerEventRecord) (application.ApplyPartnerEventResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.inbox[record.EventID]; ok {
		if existing.fingerprint != record.PayloadFingerprint {
			return application.ApplyPartnerEventResult{}, application.ErrInboxConflict
		}

		order, ok := s.orders[existing.orderID]
		if !ok {
			return application.ApplyPartnerEventResult{}, application.ErrOrderNotFound
		}

		return application.ApplyPartnerEventResult{
			Order:     copyOrder(order),
			Duplicate: true,
		}, nil
	}

	order, ok := s.orders[record.OrderID]
	if !ok {
		return application.ApplyPartnerEventResult{}, application.ErrOrderNotFound
	}

	changed, err := order.TransitionTo(record.NewStatus)
	if err != nil {
		return application.ApplyPartnerEventResult{}, err
	}

	s.orders[order.ID] = copyOrder(order)
	s.inbox[record.EventID] = inboxRecord{
		fingerprint: record.PayloadFingerprint,
		orderID:     order.ID,
	}

	return application.ApplyPartnerEventResult{
		Order:   copyOrder(order),
		Changed: changed,
	}, nil
}

func (s *Store) OutboxEvents() []events.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()

	copied := make([]events.Envelope, 0, len(s.outbox))
	for _, event := range s.outbox {
		copied = append(copied, copyEnvelope(event))
	}
	return copied
}

func copyOrder(order orderdomain.Order) orderdomain.Order {
	order.Items = append([]orderdomain.OrderItem(nil), order.Items...)
	for i := range order.Items {
		order.Items[i].ModifierItemIDs = append([]string(nil), order.Items[i].ModifierItemIDs...)
	}
	return order
}

func copyEnvelope(event events.Envelope) events.Envelope {
	event.Payload = append([]byte(nil), event.Payload...)
	return event
}
