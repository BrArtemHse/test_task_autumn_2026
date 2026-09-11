package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	catalogdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/domain"
)

const listRestaurantsSQL = `
SELECT id, name, cuisine, is_open
FROM catalog.restaurants
ORDER BY id
`

const restaurantExistsSQL = `
SELECT EXISTS (
    SELECT 1
    FROM catalog.restaurants
    WHERE id = $1
)
`

const getMenuSQL = `
SELECT
    category.id,
    category.name,
    item.id,
    item.name,
    item.description,
    item.price_cents,
    item.available AND NOT EXISTS (
        SELECT 1
        FROM catalog.menu_stop_list AS stop
        WHERE stop.restaurant_id = item.restaurant_id
          AND stop.menu_item_id = item.id
          AND stop.starts_at <= now()
          AND (stop.ends_at IS NULL OR stop.ends_at > now())
    ) AS available,
    modifier.id,
    modifier.name,
    modifier.price_delta_cents,
    modifier.available
FROM catalog.menu_categories AS category
LEFT JOIN catalog.menu_items AS item
    ON item.category_id = category.id
   AND item.restaurant_id = category.restaurant_id
LEFT JOIN catalog.menu_item_modifiers AS modifier
    ON modifier.menu_item_id = item.id
WHERE category.restaurant_id = $1
ORDER BY category.sort_order, category.id, item.id, modifier.id
`

const restaurantOpenSQL = "SELECT is_open FROM catalog.restaurants WHERE id = $1"

type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

func (s *Store) ListRestaurants(ctx context.Context) ([]catalogdomain.Restaurant, error) {
	rows, err := s.db.QueryContext(ctx, listRestaurantsSQL)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	restaurants := make([]catalogdomain.Restaurant, 0)
	for rows.Next() {
		var restaurant catalogdomain.Restaurant
		if err := rows.Scan(&restaurant.ID, &restaurant.Name, &restaurant.Cuisine, &restaurant.IsOpen); err != nil {
			return nil, err
		}
		restaurants = append(restaurants, restaurant)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return restaurants, nil
}

func (s *Store) GetMenu(ctx context.Context, restaurantID string) (catalogdomain.Menu, error) {
	var exists bool
	if err := s.db.QueryRowContext(ctx, restaurantExistsSQL, restaurantID).Scan(&exists); err != nil {
		return catalogdomain.Menu{}, err
	}
	if !exists {
		return catalogdomain.Menu{}, catalogdomain.ErrRestaurantNotFound
	}

	rows, err := s.db.QueryContext(ctx, getMenuSQL, restaurantID)
	if err != nil {
		return catalogdomain.Menu{}, err
	}
	defer func() { _ = rows.Close() }()

	menu := catalogdomain.Menu{
		RestaurantID: restaurantID,
		Categories:   make([]catalogdomain.MenuCategory, 0),
	}
	categoryIndexes := make(map[string]int)
	itemIndexes := make(map[string]struct {
		category int
		item     int
	})

	for rows.Next() {
		var categoryID string
		var categoryName string
		var itemID sql.NullString
		var itemName sql.NullString
		var description sql.NullString
		var priceCents sql.NullInt64
		var available sql.NullBool
		var modifierID sql.NullString
		var modifierName sql.NullString
		var priceDeltaCents sql.NullInt64
		var modifierAvailable sql.NullBool

		if err := rows.Scan(
			&categoryID,
			&categoryName,
			&itemID,
			&itemName,
			&description,
			&priceCents,
			&available,
			&modifierID,
			&modifierName,
			&priceDeltaCents,
			&modifierAvailable,
		); err != nil {
			return catalogdomain.Menu{}, err
		}

		categoryIndex, found := categoryIndexes[categoryID]
		if !found {
			categoryIndex = len(menu.Categories)
			categoryIndexes[categoryID] = categoryIndex
			menu.Categories = append(menu.Categories, catalogdomain.MenuCategory{
				ID:    categoryID,
				Name:  categoryName,
				Items: make([]catalogdomain.MenuItem, 0),
			})
		}
		if !itemID.Valid {
			continue
		}

		location, found := itemIndexes[itemID.String]
		if !found {
			location = struct {
				category int
				item     int
			}{
				category: categoryIndex,
				item:     len(menu.Categories[categoryIndex].Items),
			}
			itemIndexes[itemID.String] = location
			menu.Categories[categoryIndex].Items = append(
				menu.Categories[categoryIndex].Items,
				catalogdomain.MenuItem{
					ID:          itemID.String,
					Name:        itemName.String,
					Description: description.String,
					PriceCents:  priceCents.Int64,
					Available:   available.Bool,
					Modifiers:   make([]catalogdomain.MenuModifier, 0),
				},
			)
		}
		if modifierID.Valid {
			item := &menu.Categories[location.category].Items[location.item]
			item.Modifiers = append(item.Modifiers, catalogdomain.MenuModifier{
				ID:              modifierID.String,
				Name:            modifierName.String,
				PriceDeltaCents: priceDeltaCents.Int64,
				Available:       modifierAvailable.Bool,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return catalogdomain.Menu{}, err
	}
	return menu, nil
}

func (s *Store) ValidateOrderItems(
	ctx context.Context,
	restaurantID string,
	requested []catalogdomain.RequestedItem,
) ([]catalogdomain.ValidatedItem, error) {
	if len(requested) == 0 {
		return nil, catalogdomain.ErrInvalidMenuRequest
	}

	var isOpen bool
	if err := s.db.QueryRowContext(ctx, restaurantOpenSQL, restaurantID).Scan(&isOpen); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, catalogdomain.ErrRestaurantNotFound
		}
		return nil, err
	}
	if !isOpen {
		return nil, catalogdomain.ErrRestaurantClosed
	}

	menu, err := s.GetMenu(ctx, restaurantID)
	if err != nil {
		return nil, err
	}

	itemsByID := make(map[string]catalogdomain.MenuItem)
	for _, category := range menu.Categories {
		for _, item := range category.Items {
			itemsByID[item.ID] = item
		}
	}

	validated := make([]catalogdomain.ValidatedItem, 0, len(requested))
	for index, requestItem := range requested {
		if requestItem.MenuItemID == "" || requestItem.Quantity <= 0 {
			return nil, fmt.Errorf("%w: item %d", catalogdomain.ErrInvalidMenuRequest, index)
		}

		item, found := itemsByID[requestItem.MenuItemID]
		if !found {
			return nil, fmt.Errorf("%w: %s", catalogdomain.ErrMenuItemNotFound, requestItem.MenuItemID)
		}
		if !item.Available {
			return nil, fmt.Errorf("%w: %s", catalogdomain.ErrMenuItemUnavailable, requestItem.MenuItemID)
		}

		modifiersByID := make(map[string]catalogdomain.MenuModifier, len(item.Modifiers))
		for _, modifier := range item.Modifiers {
			modifiersByID[modifier.ID] = modifier
		}

		unitPrice := item.PriceCents
		modifierIDs := append([]string(nil), requestItem.ModifierItemIDs...)
		for _, modifierID := range modifierIDs {
			modifier, found := modifiersByID[modifierID]
			if !found || !modifier.Available {
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
