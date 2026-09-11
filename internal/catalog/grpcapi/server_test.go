package grpcapi_test

import (
	"context"
	"errors"
	"testing"

	catalogv1 "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/gen/go/catalog/v1"
	catalogdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/domain"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/grpcapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestServerListRestaurantsMapsDomainToProto(t *testing.T) {
	server := grpcapi.NewServer(&catalogStoreStub{
		restaurants: []catalogdomain.Restaurant{
			{ID: "rst-1", Name: "Pizza Roma", Cuisine: "Italian", IsOpen: true},
		},
	})

	response, err := server.ListRestaurants(context.Background(), &catalogv1.ListRestaurantsRequest{})
	if err != nil {
		t.Fatalf("ListRestaurants() error = %v", err)
	}

	if len(response.GetRestaurants()) != 1 {
		t.Fatalf("restaurants = %d, want 1", len(response.GetRestaurants()))
	}
	restaurant := response.GetRestaurants()[0]
	if restaurant.GetId() != "rst-1" {
		t.Fatalf("restaurant id = %q, want rst-1", restaurant.GetId())
	}
	if !restaurant.GetIsOpen() {
		t.Fatal("is_open = false, want true")
	}
}

func TestServerGetRestaurantMenuMapsNotFoundToStatus(t *testing.T) {
	server := grpcapi.NewServer(&catalogStoreStub{getMenuErr: catalogdomain.ErrRestaurantNotFound})

	_, err := server.GetRestaurantMenu(context.Background(), &catalogv1.GetRestaurantMenuRequest{
		RestaurantId: "missing",
	})

	if status.Code(err) != codes.NotFound {
		t.Fatalf("status code = %v, want %v; err = %v", status.Code(err), codes.NotFound, err)
	}
}

func TestServerValidateOrderItemsMapsInvalidRequestToStatus(t *testing.T) {
	server := grpcapi.NewServer(&catalogStoreStub{validateErr: catalogdomain.ErrInvalidMenuRequest})

	_, err := server.ValidateOrderItems(context.Background(), &catalogv1.ValidateOrderItemsRequest{
		RestaurantId: "rst-1",
	})

	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("status code = %v, want %v; err = %v", status.Code(err), codes.InvalidArgument, err)
	}
}

type catalogStoreStub struct {
	restaurants []catalogdomain.Restaurant
	menu        catalogdomain.Menu
	validated   []catalogdomain.ValidatedItem
	listErr     error
	getMenuErr  error
	validateErr error
}

func (s *catalogStoreStub) ListRestaurants(context.Context) ([]catalogdomain.Restaurant, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return append([]catalogdomain.Restaurant(nil), s.restaurants...), nil
}

func (s *catalogStoreStub) GetMenu(context.Context, string) (catalogdomain.Menu, error) {
	if s.getMenuErr != nil {
		return catalogdomain.Menu{}, s.getMenuErr
	}
	return s.menu, nil
}

func (s *catalogStoreStub) ValidateOrderItems(context.Context, string, []catalogdomain.RequestedItem) ([]catalogdomain.ValidatedItem, error) {
	if s.validateErr != nil {
		return nil, s.validateErr
	}
	return append([]catalogdomain.ValidatedItem(nil), s.validated...), nil
}

func TestServerMapsUnexpectedCatalogErrorToInternal(t *testing.T) {
	server := grpcapi.NewServer(&catalogStoreStub{listErr: errors.New("database down")})

	_, err := server.ListRestaurants(context.Background(), &catalogv1.ListRestaurantsRequest{})

	if status.Code(err) != codes.Internal {
		t.Fatalf("status code = %v, want %v; err = %v", status.Code(err), codes.Internal, err)
	}
}
