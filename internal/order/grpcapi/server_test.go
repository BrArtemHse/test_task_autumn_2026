package grpcapi_test

import (
	"context"
	"errors"
	"testing"

	orderv1 "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/gen/go/order/v1"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/application"
	orderdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/domain"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/grpcapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestServerCreateOrderMapsApplicationResultToProto(t *testing.T) {
	server := grpcapi.NewServer(&ordersStub{
		createResult: application.CreateOrderResult{
			Order: mustOrder(t, "ord-1", orderdomain.StatusPendingRestaurantConfirmation, 1),
		},
	})

	response, err := server.CreateOrder(context.Background(), &orderv1.CreateOrderRequest{
		RequestId:      "req-1",
		UserId:         "usr-1",
		IdempotencyKey: "idem-1",
		RestaurantId:   "rst-1",
		Items: []*orderv1.CreateOrderItem{
			{MenuItemId: "pizza", Quantity: 2, ModifierItemIds: []string{"extra-cheese"}},
		},
	})
	if err != nil {
		t.Fatalf("CreateOrder() error = %v", err)
	}

	order := response.GetOrder()
	if order.GetId() != "ord-1" {
		t.Fatalf("order id = %q, want ord-1", order.GetId())
	}
	if order.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_PENDING_RESTAURANT_CONFIRMATION {
		t.Fatalf("status = %v, want pending restaurant confirmation", order.GetStatus())
	}
	if order.GetVersion() != 1 {
		t.Fatalf("version = %d, want 1", order.GetVersion())
	}
}

func TestServerCreateOrderMapsIdempotencyConflictToAlreadyExists(t *testing.T) {
	server := grpcapi.NewServer(&ordersStub{createErr: application.ErrIdempotencyConflict})

	_, err := server.CreateOrder(context.Background(), &orderv1.CreateOrderRequest{
		UserId:         "usr-1",
		IdempotencyKey: "idem-1",
		RestaurantId:   "rst-1",
		Items:          []*orderv1.CreateOrderItem{{MenuItemId: "pizza", Quantity: 1}},
	})

	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("status code = %v, want %v; err = %v", status.Code(err), codes.AlreadyExists, err)
	}
}

func TestServerGetOrderMapsNotFoundToStatus(t *testing.T) {
	server := grpcapi.NewServer(&ordersStub{getErr: application.ErrOrderNotFound})

	_, err := server.GetOrder(context.Background(), &orderv1.GetOrderRequest{
		OrderId: "missing",
		UserId:  "usr-1",
	})

	if status.Code(err) != codes.NotFound {
		t.Fatalf("status code = %v, want %v; err = %v", status.Code(err), codes.NotFound, err)
	}
}

type ordersStub struct {
	createInput  application.CreateOrderInput
	createResult application.CreateOrderResult
	createErr    error
	getInput     application.GetOrderInput
	getOrder     orderdomain.Order
	getErr       error
}

func (s *ordersStub) CreateOrder(_ context.Context, input application.CreateOrderInput) (application.CreateOrderResult, error) {
	s.createInput = input
	if s.createErr != nil {
		return application.CreateOrderResult{}, s.createErr
	}
	return s.createResult, nil
}

func (s *ordersStub) GetOrder(_ context.Context, input application.GetOrderInput) (orderdomain.Order, error) {
	s.getInput = input
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

func TestServerCreateOrderMapsUnexpectedErrorToInternal(t *testing.T) {
	server := grpcapi.NewServer(&ordersStub{createErr: errors.New("store down")})

	_, err := server.CreateOrder(context.Background(), &orderv1.CreateOrderRequest{
		UserId:         "usr-1",
		IdempotencyKey: "idem-1",
		RestaurantId:   "rst-1",
		Items:          []*orderv1.CreateOrderItem{{MenuItemId: "pizza", Quantity: 1}},
	})

	if status.Code(err) != codes.Internal {
		t.Fatalf("status code = %v, want %v; err = %v", status.Code(err), codes.Internal, err)
	}
}
