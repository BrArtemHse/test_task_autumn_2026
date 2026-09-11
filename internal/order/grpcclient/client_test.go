package grpcclient_test

import (
	"context"
	"errors"
	"net"
	"testing"

	orderv1 "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/gen/go/order/v1"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/application"
	orderdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/domain"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/grpcapi"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/grpcclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestClientGetOrderMapsProtoToDomain(t *testing.T) {
	client := newTestClient(t, &ordersStub{
		getOrder: mustOrder(t, "ord-1", orderdomain.StatusAccepted, 2),
	})

	order, err := client.GetOrder(context.Background(), application.GetOrderInput{
		OrderID: "ord-1",
		UserID:  "usr-1",
	})
	if err != nil {
		t.Fatalf("GetOrder() error = %v", err)
	}

	if order.ID != "ord-1" {
		t.Fatalf("order id = %q, want ord-1", order.ID)
	}
	if order.Status != orderdomain.StatusAccepted {
		t.Fatalf("status = %q, want accepted", order.Status)
	}
	if order.Version != 2 {
		t.Fatalf("version = %d, want 2", order.Version)
	}
	if len(order.Items) != 1 {
		t.Fatalf("items = %d, want 1", len(order.Items))
	}
}

func TestClientCreateOrderMapsAlreadyExistsToIdempotencyConflict(t *testing.T) {
	client := newTestClient(t, &ordersStub{createErr: application.ErrIdempotencyConflict})

	_, err := client.CreateOrder(context.Background(), application.CreateOrderInput{
		UserID:         "usr-1",
		IdempotencyKey: "idem-1",
		RestaurantID:   "rst-1",
		Items:          []application.RequestedItem{{MenuItemID: "pizza", Quantity: 1}},
	})

	if !errors.Is(err, application.ErrIdempotencyConflict) {
		t.Fatalf("CreateOrder() error = %v, want %v", err, application.ErrIdempotencyConflict)
	}
}

func newTestClient(t *testing.T, orders *ordersStub) *grpcclient.Client {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	orderv1.RegisterOrderServiceServer(server, grpcapi.NewServer(orders))
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

	return grpcclient.New(orderv1.NewOrderServiceClient(conn))
}

type ordersStub struct {
	createResult application.CreateOrderResult
	createErr    error
	getOrder     orderdomain.Order
	getErr       error
}

func (s *ordersStub) CreateOrder(context.Context, application.CreateOrderInput) (application.CreateOrderResult, error) {
	if s.createErr != nil {
		return application.CreateOrderResult{}, s.createErr
	}
	return s.createResult, nil
}

func (s *ordersStub) GetOrder(context.Context, application.GetOrderInput) (orderdomain.Order, error) {
	if s.getErr != nil {
		return orderdomain.Order{}, s.getErr
	}
	return s.getOrder, nil
}

func mustOrder(t *testing.T, id string, status orderdomain.Status, version int) orderdomain.Order {
	t.Helper()

	order, err := orderdomain.NewOrder(orderdomain.NewOrderInput{
		OrderID:      id,
		UserID:       "usr-1",
		RestaurantID: "rst-1",
		Items: []orderdomain.NewOrderItemInput{
			{
				MenuItemID:      "pizza",
				Name:            "Pizza",
				UnitPriceCents:  1200,
				Quantity:        2,
				ModifierItemIDs: []string{"extra-cheese"},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewOrder() error = %v", err)
	}

	order.Status = status
	order.Version = version
	return order
}
