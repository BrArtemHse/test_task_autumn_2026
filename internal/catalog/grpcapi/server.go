package grpcapi

import (
	"context"
	"errors"

	catalogv1 "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/gen/go/catalog/v1"
	catalogdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Store interface {
	ListRestaurants(ctx context.Context) ([]catalogdomain.Restaurant, error)
	GetMenu(ctx context.Context, restaurantID string) (catalogdomain.Menu, error)
	ValidateOrderItems(ctx context.Context, restaurantID string, items []catalogdomain.RequestedItem) ([]catalogdomain.ValidatedItem, error)
}

type Server struct {
	catalogv1.UnimplementedCatalogServiceServer
	store Store
}

func NewServer(store Store) *Server {
	return &Server{store: store}
}

func (s *Server) ListRestaurants(ctx context.Context, _ *catalogv1.ListRestaurantsRequest) (*catalogv1.ListRestaurantsResponse, error) {
	restaurants, err := s.store.ListRestaurants(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, "catalog unavailable")
	}

	return &catalogv1.ListRestaurantsResponse{
		Restaurants: mapRestaurants(restaurants),
	}, nil
}

func (s *Server) GetRestaurantMenu(ctx context.Context, request *catalogv1.GetRestaurantMenuRequest) (*catalogv1.GetRestaurantMenuResponse, error) {
	if request.GetRestaurantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "restaurant_id is required")
	}

	menu, err := s.store.GetMenu(ctx, request.GetRestaurantId())
	if err != nil {
		return nil, mapCatalogError(err)
	}

	return &catalogv1.GetRestaurantMenuResponse{Menu: mapMenu(menu)}, nil
}

func (s *Server) ValidateOrderItems(ctx context.Context, request *catalogv1.ValidateOrderItemsRequest) (*catalogv1.ValidateOrderItemsResponse, error) {
	if request.GetRestaurantId() == "" || len(request.GetItems()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "restaurant_id and items are required")
	}

	items, err := s.store.ValidateOrderItems(ctx, request.GetRestaurantId(), mapRequestedItems(request.GetItems()))
	if err != nil {
		return nil, mapCatalogError(err)
	}

	var total int64
	responseItems := make([]*catalogv1.ValidatedItem, 0, len(items))
	for _, item := range items {
		total += item.LineTotalCents
		responseItems = append(responseItems, &catalogv1.ValidatedItem{
			MenuItemId:      item.MenuItemID,
			Name:            item.Name,
			UnitPriceCents:  item.UnitPriceCents,
			Quantity:        item.Quantity,
			ModifierItemIds: append([]string(nil), item.ModifierItemIDs...),
			LineTotalCents:  item.LineTotalCents,
		})
	}

	return &catalogv1.ValidateOrderItemsResponse{
		Items:      responseItems,
		TotalCents: total,
	}, nil
}

func mapCatalogError(err error) error {
	switch {
	case errors.Is(err, catalogdomain.ErrRestaurantNotFound):
		return status.Error(codes.NotFound, "restaurant not found")
	case errors.Is(err, catalogdomain.ErrInvalidMenuRequest),
		errors.Is(err, catalogdomain.ErrRestaurantClosed),
		errors.Is(err, catalogdomain.ErrMenuItemNotFound),
		errors.Is(err, catalogdomain.ErrMenuItemUnavailable),
		errors.Is(err, catalogdomain.ErrMenuModifierNotFound):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, "catalog unavailable")
	}
}

func mapRestaurants(restaurants []catalogdomain.Restaurant) []*catalogv1.Restaurant {
	result := make([]*catalogv1.Restaurant, 0, len(restaurants))
	for _, restaurant := range restaurants {
		result = append(result, &catalogv1.Restaurant{
			Id:      restaurant.ID,
			Name:    restaurant.Name,
			Cuisine: restaurant.Cuisine,
			IsOpen:  restaurant.IsOpen,
		})
	}
	return result
}

func mapMenu(menu catalogdomain.Menu) *catalogv1.Menu {
	categories := make([]*catalogv1.MenuCategory, 0, len(menu.Categories))
	for _, category := range menu.Categories {
		items := make([]*catalogv1.MenuItem, 0, len(category.Items))
		for _, item := range category.Items {
			modifiers := make([]*catalogv1.MenuModifier, 0, len(item.Modifiers))
			for _, modifier := range item.Modifiers {
				modifiers = append(modifiers, &catalogv1.MenuModifier{
					Id:              modifier.ID,
					Name:            modifier.Name,
					PriceDeltaCents: modifier.PriceDeltaCents,
				})
			}
			items = append(items, &catalogv1.MenuItem{
				Id:          item.ID,
				Name:        item.Name,
				Description: item.Description,
				PriceCents:  item.PriceCents,
				Available:   item.Available,
				Modifiers:   modifiers,
			})
		}
		categories = append(categories, &catalogv1.MenuCategory{
			Id:    category.ID,
			Name:  category.Name,
			Items: items,
		})
	}

	return &catalogv1.Menu{
		RestaurantId: menu.RestaurantID,
		Categories:   categories,
	}
}

func mapRequestedItems(items []*catalogv1.RequestedItem) []catalogdomain.RequestedItem {
	result := make([]catalogdomain.RequestedItem, 0, len(items))
	for _, item := range items {
		result = append(result, catalogdomain.RequestedItem{
			MenuItemID:      item.GetMenuItemId(),
			Quantity:        item.GetQuantity(),
			ModifierItemIDs: append([]string(nil), item.GetModifierItemIds()...),
		})
	}
	return result
}
