package catalogmemory

import (
	"context"

	catalogdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/domain"
	orderapp "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/application"
	orderdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/domain"
)

type Store interface {
	ValidateOrderItems(ctx context.Context, restaurantID string, items []catalogdomain.RequestedItem) ([]catalogdomain.ValidatedItem, error)
}

type Adapter struct {
	store Store
}

func New(store Store) *Adapter {
	return &Adapter{store: store}
}

func (a *Adapter) ValidateOrderItems(ctx context.Context, restaurantID string, items []orderapp.RequestedItem) ([]orderdomain.NewOrderItemInput, error) {
	requested := make([]catalogdomain.RequestedItem, 0, len(items))
	for _, item := range items {
		requested = append(requested, catalogdomain.RequestedItem{
			MenuItemID:      item.MenuItemID,
			Quantity:        item.Quantity,
			ModifierItemIDs: append([]string(nil), item.ModifierItemIDs...),
		})
	}

	validated, err := a.store.ValidateOrderItems(ctx, restaurantID, requested)
	if err != nil {
		return nil, err
	}

	result := make([]orderdomain.NewOrderItemInput, 0, len(validated))
	for _, item := range validated {
		result = append(result, orderdomain.NewOrderItemInput{
			MenuItemID:      item.MenuItemID,
			Name:            item.Name,
			UnitPriceCents:  item.UnitPriceCents,
			Quantity:        item.Quantity,
			ModifierItemIDs: append([]string(nil), item.ModifierItemIDs...),
		})
	}

	return result, nil
}
