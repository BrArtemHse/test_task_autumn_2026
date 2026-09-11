package grpcclient

import (
	"context"

	catalogv1 "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/gen/go/catalog/v1"
	catalogdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/domain"
	orderapp "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/application"
	orderdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Client struct {
	client catalogv1.CatalogServiceClient
}

func New(client catalogv1.CatalogServiceClient) *Client {
	return &Client{client: client}
}

func (c *Client) ListRestaurants(ctx context.Context) ([]catalogdomain.Restaurant, error) {
	response, err := c.client.ListRestaurants(ctx, &catalogv1.ListRestaurantsRequest{})
	if err != nil {
		return nil, mapStatusError(err)
	}

	restaurants := make([]catalogdomain.Restaurant, 0, len(response.GetRestaurants()))
	for _, restaurant := range response.GetRestaurants() {
		restaurants = append(restaurants, catalogdomain.Restaurant{
			ID:      restaurant.GetId(),
			Name:    restaurant.GetName(),
			Cuisine: restaurant.GetCuisine(),
			IsOpen:  restaurant.GetIsOpen(),
		})
	}
	return restaurants, nil
}

func (c *Client) GetMenu(ctx context.Context, restaurantID string) (catalogdomain.Menu, error) {
	response, err := c.client.GetRestaurantMenu(ctx, &catalogv1.GetRestaurantMenuRequest{RestaurantId: restaurantID})
	if err != nil {
		return catalogdomain.Menu{}, mapStatusError(err)
	}
	return mapMenu(response.GetMenu()), nil
}

func (c *Client) ValidateOrderItems(ctx context.Context, restaurantID string, items []orderapp.RequestedItem) ([]orderdomain.NewOrderItemInput, error) {
	response, err := c.client.ValidateOrderItems(ctx, &catalogv1.ValidateOrderItemsRequest{
		RestaurantId: restaurantID,
		Items:        mapRequestedItems(items),
	})
	if err != nil {
		return nil, mapValidateOrderItemsError(err)
	}

	validated := make([]orderdomain.NewOrderItemInput, 0, len(response.GetItems()))
	for _, item := range response.GetItems() {
		validated = append(validated, orderdomain.NewOrderItemInput{
			MenuItemID:      item.GetMenuItemId(),
			Name:            item.GetName(),
			UnitPriceCents:  item.GetUnitPriceCents(),
			Quantity:        item.GetQuantity(),
			ModifierItemIDs: append([]string(nil), item.GetModifierItemIds()...),
		})
	}
	return validated, nil
}

func mapValidateOrderItemsError(err error) error {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.NotFound:
		return orderapp.ErrInvalidCreateOrder
	default:
		return err
	}
}

func mapStatusError(err error) error {
	switch status.Code(err) {
	case codes.NotFound:
		return catalogdomain.ErrRestaurantNotFound
	case codes.InvalidArgument:
		return catalogdomain.ErrInvalidMenuRequest
	default:
		return err
	}
}

func mapRequestedItems(items []orderapp.RequestedItem) []*catalogv1.RequestedItem {
	result := make([]*catalogv1.RequestedItem, 0, len(items))
	for _, item := range items {
		result = append(result, &catalogv1.RequestedItem{
			MenuItemId:      item.MenuItemID,
			Quantity:        item.Quantity,
			ModifierItemIds: append([]string(nil), item.ModifierItemIDs...),
		})
	}
	return result
}

func mapMenu(menu *catalogv1.Menu) catalogdomain.Menu {
	if menu == nil {
		return catalogdomain.Menu{}
	}

	categories := make([]catalogdomain.MenuCategory, 0, len(menu.GetCategories()))
	for _, category := range menu.GetCategories() {
		items := make([]catalogdomain.MenuItem, 0, len(category.GetItems()))
		for _, item := range category.GetItems() {
			modifiers := make([]catalogdomain.MenuModifier, 0, len(item.GetModifiers()))
			for _, modifier := range item.GetModifiers() {
				modifiers = append(modifiers, catalogdomain.MenuModifier{
					ID:              modifier.GetId(),
					Name:            modifier.GetName(),
					PriceDeltaCents: modifier.GetPriceDeltaCents(),
					Available:       true,
				})
			}
			items = append(items, catalogdomain.MenuItem{
				ID:          item.GetId(),
				Name:        item.GetName(),
				Description: item.GetDescription(),
				PriceCents:  item.GetPriceCents(),
				Available:   item.GetAvailable(),
				Modifiers:   modifiers,
			})
		}
		categories = append(categories, catalogdomain.MenuCategory{
			ID:    category.GetId(),
			Name:  category.GetName(),
			Items: items,
		})
	}

	return catalogdomain.Menu{
		RestaurantID: menu.GetRestaurantId(),
		Categories:   categories,
	}
}
