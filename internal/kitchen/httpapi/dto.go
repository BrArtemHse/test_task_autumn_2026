package httpapi

import (
	catalogdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/domain"
	orderapp "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/application"
	orderdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/domain"
)

type restaurantResponse struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Cuisine string `json:"cuisine"`
	IsOpen  bool   `json:"isOpen"`
}

type menuResponse struct {
	RestaurantID string                 `json:"restaurantId"`
	Categories   []menuCategoryResponse `json:"categories"`
}

type menuCategoryResponse struct {
	ID    string             `json:"id"`
	Name  string             `json:"name"`
	Items []menuItemResponse `json:"items"`
}

type menuItemResponse struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	PriceCents  int64                  `json:"priceCents"`
	Available   bool                   `json:"available"`
	Modifiers   []menuModifierResponse `json:"modifiers,omitempty"`
}

type menuModifierResponse struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	PriceDeltaCents int64  `json:"priceDeltaCents"`
}

type createOrderRequest struct {
	RestaurantID string                   `json:"restaurantId"`
	Items        []createOrderItemRequest `json:"items"`
}

type createOrderItemRequest struct {
	MenuItemID      string   `json:"menuItemId"`
	Quantity        int32    `json:"quantity"`
	ModifierItemIDs []string `json:"modifierItemIds,omitempty"`
}

type orderResponse struct {
	ID           string              `json:"id"`
	UserID       string              `json:"userId"`
	RestaurantID string              `json:"restaurantId"`
	Status       string              `json:"status"`
	Items        []orderItemResponse `json:"items"`
	TotalCents   int64               `json:"totalCents"`
	Version      int                 `json:"version"`
}

type orderItemResponse struct {
	MenuItemID      string   `json:"menuItemId"`
	Name            string   `json:"name"`
	UnitPriceCents  int64    `json:"unitPriceCents"`
	Quantity        int32    `json:"quantity"`
	ModifierItemIDs []string `json:"modifierItemIds,omitempty"`
	LineTotalCents  int64    `json:"lineTotalCents"`
}

type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func mapRestaurants(restaurants []catalogdomain.Restaurant) []restaurantResponse {
	result := make([]restaurantResponse, 0, len(restaurants))
	for _, restaurant := range restaurants {
		result = append(result, restaurantResponse{
			ID:      restaurant.ID,
			Name:    restaurant.Name,
			Cuisine: restaurant.Cuisine,
			IsOpen:  restaurant.IsOpen,
		})
	}
	return result
}

func mapMenu(menu catalogdomain.Menu) menuResponse {
	categories := make([]menuCategoryResponse, 0, len(menu.Categories))
	for _, category := range menu.Categories {
		items := make([]menuItemResponse, 0, len(category.Items))
		for _, item := range category.Items {
			modifiers := make([]menuModifierResponse, 0, len(item.Modifiers))
			for _, modifier := range item.Modifiers {
				modifiers = append(modifiers, menuModifierResponse{
					ID:              modifier.ID,
					Name:            modifier.Name,
					PriceDeltaCents: modifier.PriceDeltaCents,
				})
			}
			items = append(items, menuItemResponse{
				ID:          item.ID,
				Name:        item.Name,
				Description: item.Description,
				PriceCents:  item.PriceCents,
				Available:   item.Available,
				Modifiers:   modifiers,
			})
		}
		categories = append(categories, menuCategoryResponse{
			ID:    category.ID,
			Name:  category.Name,
			Items: items,
		})
	}

	return menuResponse{
		RestaurantID: menu.RestaurantID,
		Categories:   categories,
	}
}

func mapRequestedItems(items []createOrderItemRequest) []orderapp.RequestedItem {
	result := make([]orderapp.RequestedItem, 0, len(items))
	for _, item := range items {
		result = append(result, orderapp.RequestedItem{
			MenuItemID:      item.MenuItemID,
			Quantity:        item.Quantity,
			ModifierItemIDs: append([]string(nil), item.ModifierItemIDs...),
		})
	}
	return result
}

func mapOrder(order orderdomain.Order) orderResponse {
	items := make([]orderItemResponse, 0, len(order.Items))
	for _, item := range order.Items {
		items = append(items, orderItemResponse{
			MenuItemID:      item.MenuItemID,
			Name:            item.Name,
			UnitPriceCents:  item.UnitPriceCents,
			Quantity:        item.Quantity,
			ModifierItemIDs: append([]string(nil), item.ModifierItemIDs...),
			LineTotalCents:  item.LineTotalCents,
		})
	}

	return orderResponse{
		ID:           order.ID,
		UserID:       order.UserID,
		RestaurantID: order.RestaurantID,
		Status:       string(order.Status),
		Items:        items,
		TotalCents:   order.TotalCents,
		Version:      order.Version,
	}
}
