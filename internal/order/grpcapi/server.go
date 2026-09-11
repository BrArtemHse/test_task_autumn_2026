package grpcapi

import (
	"context"
	"errors"

	orderv1 "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/gen/go/order/v1"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/application"
	orderdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Orders interface {
	CreateOrder(ctx context.Context, input application.CreateOrderInput) (application.CreateOrderResult, error)
	GetOrder(ctx context.Context, input application.GetOrderInput) (orderdomain.Order, error)
}

type Server struct {
	orderv1.UnimplementedOrderServiceServer
	orders Orders
}

func NewServer(orders Orders) *Server {
	return &Server{orders: orders}
}

func (s *Server) CreateOrder(ctx context.Context, request *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
	if request.GetUserId() == "" || request.GetIdempotencyKey() == "" || request.GetRestaurantId() == "" || len(request.GetItems()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "user_id, idempotency_key, restaurant_id and items are required")
	}

	result, err := s.orders.CreateOrder(ctx, application.CreateOrderInput{
		RequestID:      request.GetRequestId(),
		UserID:         request.GetUserId(),
		IdempotencyKey: request.GetIdempotencyKey(),
		RestaurantID:   request.GetRestaurantId(),
		Items:          mapCreateOrderItems(request.GetItems()),
	})
	if err != nil {
		return nil, mapApplicationError(err)
	}

	return &orderv1.CreateOrderResponse{
		Order:                mapOrder(result.Order),
		ReusedIdempotencyKey: result.ReusedIdempotencyKey,
	}, nil
}

func (s *Server) GetOrder(ctx context.Context, request *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	if request.GetOrderId() == "" || request.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "order_id and user_id are required")
	}

	order, err := s.orders.GetOrder(ctx, application.GetOrderInput{
		OrderID: request.GetOrderId(),
		UserID:  request.GetUserId(),
	})
	if err != nil {
		return nil, mapApplicationError(err)
	}

	return &orderv1.GetOrderResponse{Order: mapOrder(order)}, nil
}

func mapApplicationError(err error) error {
	switch {
	case errors.Is(err, application.ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, "idempotency key was reused with a different payload")
	case errors.Is(err, application.ErrOrderNotFound):
		return status.Error(codes.NotFound, "order not found")
	case errors.Is(err, application.ErrInvalidCreateOrder):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, orderdomain.ErrInvalidStatusTransition):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, "order service unavailable")
	}
}

func mapCreateOrderItems(items []*orderv1.CreateOrderItem) []application.RequestedItem {
	result := make([]application.RequestedItem, 0, len(items))
	for _, item := range items {
		result = append(result, application.RequestedItem{
			MenuItemID:      item.GetMenuItemId(),
			Quantity:        item.GetQuantity(),
			ModifierItemIDs: append([]string(nil), item.GetModifierItemIds()...),
		})
	}
	return result
}

func mapOrder(order orderdomain.Order) *orderv1.Order {
	items := make([]*orderv1.OrderItem, 0, len(order.Items))
	for _, item := range order.Items {
		items = append(items, &orderv1.OrderItem{
			MenuItemId:      item.MenuItemID,
			Name:            item.Name,
			UnitPriceCents:  item.UnitPriceCents,
			Quantity:        item.Quantity,
			ModifierItemIds: append([]string(nil), item.ModifierItemIDs...),
			LineTotalCents:  item.LineTotalCents,
		})
	}

	return &orderv1.Order{
		Id:           order.ID,
		UserId:       order.UserID,
		RestaurantId: order.RestaurantID,
		Status:       mapStatus(order.Status),
		Items:        items,
		TotalCents:   order.TotalCents,
		Version:      int64(order.Version),
	}
}

func mapStatus(status orderdomain.Status) orderv1.OrderStatus {
	switch status {
	case orderdomain.StatusPendingRestaurantConfirmation:
		return orderv1.OrderStatus_ORDER_STATUS_PENDING_RESTAURANT_CONFIRMATION
	case orderdomain.StatusAccepted:
		return orderv1.OrderStatus_ORDER_STATUS_ACCEPTED
	case orderdomain.StatusRejected:
		return orderv1.OrderStatus_ORDER_STATUS_REJECTED
	case orderdomain.StatusPreparing:
		return orderv1.OrderStatus_ORDER_STATUS_PREPARING
	case orderdomain.StatusReady:
		return orderv1.OrderStatus_ORDER_STATUS_READY
	case orderdomain.StatusCompleted:
		return orderv1.OrderStatus_ORDER_STATUS_COMPLETED
	case orderdomain.StatusCancelled:
		return orderv1.OrderStatus_ORDER_STATUS_CANCELLED
	default:
		return orderv1.OrderStatus_ORDER_STATUS_UNSPECIFIED
	}
}
