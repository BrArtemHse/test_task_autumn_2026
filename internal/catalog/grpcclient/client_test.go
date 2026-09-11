package grpcclient_test

import (
	"context"
	"errors"
	"net"
	"testing"

	catalogv1 "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/gen/go/catalog/v1"
	catalogdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/domain"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/grpcapi"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/grpcclient"
	orderapp "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/application"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientValidateOrderItemsMapsProtoToApplicationPort(t *testing.T) {
	store := &catalogStoreStub{
		validated: []catalogdomain.ValidatedItem{
			{
				MenuItemID:      "pizza",
				Name:            "Pizza",
				UnitPriceCents:  1200,
				Quantity:        2,
				ModifierItemIDs: []string{"extra-cheese"},
				LineTotalCents:  2400,
			},
		},
	}
	client := newTestClient(t, store)

	items, err := client.ValidateOrderItems(context.Background(), "rst-1", []orderapp.RequestedItem{
		{MenuItemID: "pizza", Quantity: 2, ModifierItemIDs: []string{"extra-cheese"}},
	})
	if err != nil {
		t.Fatalf("ValidateOrderItems() error = %v", err)
	}

	if store.validateRestaurantID != "rst-1" {
		t.Fatalf("restaurant id = %q, want rst-1", store.validateRestaurantID)
	}
	if len(store.validateItems) != 1 {
		t.Fatalf("requested items = %d, want 1", len(store.validateItems))
	}
	if len(items) != 1 {
		t.Fatalf("validated items = %d, want 1", len(items))
	}
	if items[0].MenuItemID != "pizza" || items[0].Name != "Pizza" {
		t.Fatalf("validated item = %#v", items[0])
	}
}

func TestClientValidateOrderItemsMapsCatalogValidationStatusToOrderInputError(t *testing.T) {
	tests := []struct {
		name        string
		validateErr error
	}{
		{name: "invalid item", validateErr: catalogdomain.ErrMenuItemUnavailable},
		{name: "restaurant not found", validateErr: catalogdomain.ErrRestaurantNotFound},
		{name: "restaurant closed", validateErr: catalogdomain.ErrRestaurantClosed},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newTestClient(t, &catalogStoreStub{validateErr: test.validateErr})

			_, err := client.ValidateOrderItems(context.Background(), "rst-1", []orderapp.RequestedItem{
				{MenuItemID: "unavailable", Quantity: 1},
			})

			if !errors.Is(err, orderapp.ErrInvalidCreateOrder) {
				t.Fatalf("ValidateOrderItems() error = %v, want %v", err, orderapp.ErrInvalidCreateOrder)
			}
		})
	}
}

func TestClientGetMenuMapsNotFoundStatusToDomainError(t *testing.T) {
	client := newTestClient(t, &catalogStoreStub{getMenuErr: catalogdomain.ErrRestaurantNotFound})

	_, err := client.GetMenu(context.Background(), "missing")

	if err != catalogdomain.ErrRestaurantNotFound {
		t.Fatalf("GetMenu() error = %v, want %v", err, catalogdomain.ErrRestaurantNotFound)
	}
}

func newTestClient(t *testing.T, store *catalogStoreStub) *grpcclient.Client {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	catalogv1.RegisterCatalogServiceServer(server, grpcapi.NewServer(store))
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return grpcclient.New(catalogv1.NewCatalogServiceClient(conn))
}

type catalogStoreStub struct {
	restaurants          []catalogdomain.Restaurant
	menu                 catalogdomain.Menu
	validated            []catalogdomain.ValidatedItem
	getMenuErr           error
	validateErr          error
	validateRestaurantID string
	validateItems        []catalogdomain.RequestedItem
}

func (s *catalogStoreStub) ListRestaurants(context.Context) ([]catalogdomain.Restaurant, error) {
	return append([]catalogdomain.Restaurant(nil), s.restaurants...), nil
}

func (s *catalogStoreStub) GetMenu(context.Context, string) (catalogdomain.Menu, error) {
	if s.getMenuErr != nil {
		return catalogdomain.Menu{}, s.getMenuErr
	}
	return s.menu, nil
}

func (s *catalogStoreStub) ValidateOrderItems(_ context.Context, restaurantID string, items []catalogdomain.RequestedItem) ([]catalogdomain.ValidatedItem, error) {
	s.validateRestaurantID = restaurantID
	s.validateItems = append([]catalogdomain.RequestedItem(nil), items...)
	if s.validateErr != nil {
		return nil, s.validateErr
	}
	if s.validated != nil {
		return append([]catalogdomain.ValidatedItem(nil), s.validated...), nil
	}
	return nil, catalogdomain.ErrInvalidMenuRequest
}
