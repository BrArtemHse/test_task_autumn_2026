package memory

import (
	"context"
	"fmt"

	catalogdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/domain"
)

type Store struct {
	restaurants []catalogdomain.Restaurant
	menus       map[string]catalogdomain.Menu
}

func NewFixtureStore() *Store {
	return &Store{
		restaurants: []catalogdomain.Restaurant{
			{
				ID:      "rst-pizza-1",
				Name:    "Pizza Roma",
				Cuisine: "Italian",
				IsOpen:  true,
			},
		},
		menus: map[string]catalogdomain.Menu{
			"rst-pizza-1": {
				RestaurantID: "rst-pizza-1",
				Categories: []catalogdomain.MenuCategory{
					{
						ID:   "cat-pizza",
						Name: "Pizza",
						Items: []catalogdomain.MenuItem{
							{
								ID:          "item-margherita",
								Name:        "Margherita",
								Description: "Tomato sauce, mozzarella, basil",
								PriceCents:  69000,
								Available:   true,
								Modifiers: []catalogdomain.MenuModifier{
									{
										ID:              "mod-extra-cheese",
										Name:            "Extra cheese",
										PriceDeltaCents: 0,
										Available:       true,
									},
								},
							},
							{
								ID:          "item-lasagna",
								Name:        "Lasagna",
								Description: "Temporarily unavailable",
								PriceCents:  82000,
								Available:   false,
							},
						},
					},
					{
						ID:   "cat-drinks",
						Name: "Drinks",
						Items: []catalogdomain.MenuItem{
							{
								ID:          "item-lemonade",
								Name:        "Lemonade",
								Description: "House lemonade",
								PriceCents:  22000,
								Available:   true,
							},
						},
					},
				},
			},
		},
	}
}

func (s *Store) ListRestaurants(_ context.Context) ([]catalogdomain.Restaurant, error) {
	return append([]catalogdomain.Restaurant(nil), s.restaurants...), nil
}

func (s *Store) GetMenu(_ context.Context, restaurantID string) (catalogdomain.Menu, error) {
	menu, ok := s.menus[restaurantID]
	if !ok {
		return catalogdomain.Menu{}, catalogdomain.ErrRestaurantNotFound
	}
	return copyMenu(menu), nil
}

func (s *Store) ValidateOrderItems(_ context.Context, restaurantID string, requested []catalogdomain.RequestedItem) ([]catalogdomain.ValidatedItem, error) {
	if len(requested) == 0 {
		return nil, catalogdomain.ErrInvalidMenuRequest
	}

	menu, ok := s.menus[restaurantID]
	if !ok {
		return nil, catalogdomain.ErrRestaurantNotFound
	}
	for _, restaurant := range s.restaurants {
		if restaurant.ID == restaurantID && !restaurant.IsOpen {
			return nil, catalogdomain.ErrRestaurantClosed
		}
	}

	itemsByID := make(map[string]catalogdomain.MenuItem)
	modifiersByItemID := make(map[string]map[string]catalogdomain.MenuModifier)
	for _, category := range menu.Categories {
		for _, item := range category.Items {
			itemsByID[item.ID] = item
			modifiers := make(map[string]catalogdomain.MenuModifier)
			for _, modifier := range item.Modifiers {
				modifiers[modifier.ID] = modifier
			}
			modifiersByItemID[item.ID] = modifiers
		}
	}

	validated := make([]catalogdomain.ValidatedItem, 0, len(requested))
	for i, requestItem := range requested {
		if requestItem.MenuItemID == "" || requestItem.Quantity <= 0 {
			return nil, fmt.Errorf("%w: item %d", catalogdomain.ErrInvalidMenuRequest, i)
		}

		item, ok := itemsByID[requestItem.MenuItemID]
		if !ok {
			return nil, fmt.Errorf("%w: %s", catalogdomain.ErrMenuItemNotFound, requestItem.MenuItemID)
		}
		if !item.Available {
			return nil, fmt.Errorf("%w: %s", catalogdomain.ErrMenuItemUnavailable, requestItem.MenuItemID)
		}

		unitPrice := item.PriceCents
		modifierIDs := append([]string(nil), requestItem.ModifierItemIDs...)
		for _, modifierID := range modifierIDs {
			modifier, ok := modifiersByItemID[item.ID][modifierID]
			if !ok || !modifier.Available {
				return nil, fmt.Errorf("%w: %s", catalogdomain.ErrMenuModifierNotFound, modifierID)
			}
			unitPrice += modifier.PriceDeltaCents
		}

		validated = append(validated, catalogdomain.ValidatedItem{
			MenuItemID:      item.ID,
			Name:            item.Name,
			UnitPriceCents:  unitPrice,
			Quantity:        requestItem.Quantity,
			ModifierItemIDs: modifierIDs,
			LineTotalCents:  unitPrice * int64(requestItem.Quantity),
		})
	}

	return validated, nil
}

func copyMenu(menu catalogdomain.Menu) catalogdomain.Menu {
	menu.Categories = append([]catalogdomain.MenuCategory(nil), menu.Categories...)
	for categoryIndex := range menu.Categories {
		menu.Categories[categoryIndex].Items = append([]catalogdomain.MenuItem(nil), menu.Categories[categoryIndex].Items...)
		for itemIndex := range menu.Categories[categoryIndex].Items {
			menu.Categories[categoryIndex].Items[itemIndex].Modifiers = append(
				[]catalogdomain.MenuModifier(nil),
				menu.Categories[categoryIndex].Items[itemIndex].Modifiers...,
			)
		}
	}
	return menu
}
