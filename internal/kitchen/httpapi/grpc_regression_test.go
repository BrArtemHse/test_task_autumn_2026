package httpapi

import (
	"context"
	"net"
	"net/http"
	"reflect"
	"testing"

	catalogv1 "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/gen/go/catalog/v1"
	orderv1 "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/gen/go/order/v1"
	catalogdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/domain"
	cataloggrpcapi "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/grpcapi"
	cataloggrpcclient "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/grpcclient"
	catalogmemory "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/memory"
	orderapp "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/application"
	ordergrpcapi "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/grpcapi"
	ordergrpcclient "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/grpcclient"
	ordermemory "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/memory"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/id"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestHandlerMapsCatalogValidationAcrossBothGRPCHops(t *testing.T) {
	for _, wantError := range []error{
		catalogdomain.ErrRestaurantClosed,
		catalogdomain.ErrRestaurantNotFound,
		catalogdomain.ErrMenuItemUnavailable,
	} {
		t.Run(wantError.Error(), func(t *testing.T) {
			handler, store, _ := newGRPCTestHandler(t, &validationCatalog{
				Store: catalogmemory.NewFixtureStore(), err: wantError,
			})
			result := performCreateOrder(handler, "grpc-order-key", grpcOrderBody)
			if result.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body = %s", result.Code, result.Body)
			}
			if len(store.OutboxEvents()) != 0 {
				t.Fatal("rejected order produced an outbox event")
			}
		})
	}
}

func TestHandlerReplaysOrderWhileCatalogGRPCIsOffline(t *testing.T) {
	handler, store, stopCatalog := newGRPCTestHandler(t, &validationCatalog{Store: catalogmemory.NewFixtureStore()})
	first := performCreateOrder(handler, "grpc-order-key", grpcOrderBody)
	if first.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body = %s", first.Code, first.Body)
	}
	stopCatalog()

	replay := performCreateOrder(handler, "grpc-order-key", grpcOrderBody)
	if replay.Code != http.StatusOK {
		t.Fatalf("replay status = %d, want 200; body = %s", replay.Code, replay.Body)
	}
	var originalOrder, replayedOrder orderResponse
	decodeJSON(t, first, &originalOrder)
	decodeJSON(t, replay, &replayedOrder)
	if !reflect.DeepEqual(originalOrder, replayedOrder) {
		t.Fatal("replay changed the stored order")
	}

	conflictBody := []byte("{\"restaurantId\":\"rst-pizza-1\",\"items\":[{\"menuItemId\":\"item-margherita\",\"quantity\":2}]}")
	conflict := performCreateOrder(handler, "grpc-order-key", conflictBody)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d, want 409; body = %s", conflict.Code, conflict.Body)
	}
	if len(store.OutboxEvents()) != 1 {
		t.Fatal("replay or conflict produced another outbox event")
	}
}

var grpcOrderBody = []byte("{\"restaurantId\":\"rst-pizza-1\",\"items\":[{\"menuItemId\":\"item-margherita\",\"quantity\":1}]}")

type validationCatalog struct {
	*catalogmemory.Store
	err error
}

func (c *validationCatalog) ValidateOrderItems(ctx context.Context, restaurantID string, items []catalogdomain.RequestedItem) ([]catalogdomain.ValidatedItem, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.Store.ValidateOrderItems(ctx, restaurantID, items)
}

func newGRPCTestHandler(t *testing.T, catalog *validationCatalog) (http.Handler, *ordermemory.Store, func()) {
	t.Helper()
	catalogConn, stopCatalog := newGRPCTestConnection(t, func(server *grpc.Server) {
		catalogv1.RegisterCatalogServiceServer(server, cataloggrpcapi.NewServer(catalog))
	})
	catalogClient := cataloggrpcclient.New(catalogv1.NewCatalogServiceClient(catalogConn))
	store := ordermemory.NewStore()
	service := orderapp.NewService(orderapp.ServiceConfig{Catalog: catalogClient, Store: store, NewID: id.NewString})
	orderConn, _ := newGRPCTestConnection(t, func(server *grpc.Server) {
		orderv1.RegisterOrderServiceServer(server, ordergrpcapi.NewServer(service))
	})
	return NewHandler(Dependencies{
		Catalog: catalogClient,
		Orders:  ordergrpcclient.New(orderv1.NewOrderServiceClient(orderConn)),
	}), store, stopCatalog
}

func newGRPCTestConnection(t *testing.T, register func(*grpc.Server)) (*grpc.ClientConn, func()) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = listener.Close() })
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, server.Stop
}
