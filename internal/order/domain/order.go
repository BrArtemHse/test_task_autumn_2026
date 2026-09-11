package orderdomain

import (
	"errors"
	"fmt"
)

var (
	ErrEmptyOrder              = errors.New("empty order")
	ErrInvalidOrder            = errors.New("invalid order")
	ErrInvalidOrderItem        = errors.New("invalid order item")
	ErrInvalidStatusTransition = errors.New("invalid status transition")
)

type NewOrderInput struct {
	OrderID      string
	UserID       string
	RestaurantID string
	Items        []NewOrderItemInput
}

type NewOrderItemInput struct {
	MenuItemID      string
	Name            string
	UnitPriceCents  int64
	Quantity        int32
	ModifierItemIDs []string
}

type Order struct {
	ID           string
	UserID       string
	RestaurantID string
	Status       Status
	Items        []OrderItem
	TotalCents   int64
	Version      int
}

type OrderItem struct {
	MenuItemID      string
	Name            string
	UnitPriceCents  int64
	Quantity        int32
	ModifierItemIDs []string
	LineTotalCents  int64
}

func NewOrder(input NewOrderInput) (Order, error) {
	if input.OrderID == "" || input.UserID == "" || input.RestaurantID == "" {
		return Order{}, ErrInvalidOrder
	}
	if len(input.Items) == 0 {
		return Order{}, ErrEmptyOrder
	}

	items := make([]OrderItem, 0, len(input.Items))
	var total int64

	for i, item := range input.Items {
		if item.MenuItemID == "" || item.Name == "" || item.Quantity <= 0 || item.UnitPriceCents <= 0 {
			return Order{}, fmt.Errorf("%w at index %d", ErrInvalidOrderItem, i)
		}

		lineTotal := item.UnitPriceCents * int64(item.Quantity)
		modifiers := append([]string(nil), item.ModifierItemIDs...)
		items = append(items, OrderItem{
			MenuItemID:      item.MenuItemID,
			Name:            item.Name,
			UnitPriceCents:  item.UnitPriceCents,
			Quantity:        item.Quantity,
			ModifierItemIDs: modifiers,
			LineTotalCents:  lineTotal,
		})
		total += lineTotal
	}

	return Order{
		ID:           input.OrderID,
		UserID:       input.UserID,
		RestaurantID: input.RestaurantID,
		Status:       StatusPendingRestaurantConfirmation,
		Items:        items,
		TotalCents:   total,
		Version:      1,
	}, nil
}

func (o *Order) TransitionTo(next Status) (bool, error) {
	if o.Status == next {
		return false, nil
	}

	if !canTransition(o.Status, next) {
		return false, fmt.Errorf("%w: %s -> %s", ErrInvalidStatusTransition, o.Status, next)
	}

	o.Status = next
	o.Version++
	return true, nil
}
