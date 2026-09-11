package grpcclient

import (
	"context"

	orderv1 "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/gen/go/order/v1"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/application"
	orderdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Client struct {
	client orderv1.OrderServiceClient
}

func New(client orderv1.OrderServiceClient) *Client {
	return &Client{client: client}
}

func (c *Client) CreateOrder(ctx context.Context, input application.CreateOrderInput) (application.CreateOrderResult, error) {
	response, err := c.client.CreateOrder(ctx, &orderv1.CreateOrderRequest{
		RequestId:      input.RequestID,
		UserId:         input.UserID,
		IdempotencyKey: input.IdempotencyKey,
		RestaurantId:   input.RestaurantID,
		Items:          mapCreateOrderItems(input.Items),
	})
	if err != nil {
		return application.CreateOrderResult{}, mapStatusError(err)
	}

	return application.CreateOrderResult{
		Order:                mapOrder(response.GetOrder()),
		ReusedIdempotencyKey: response.GetReusedIdempotencyKey(),
	}, nil
}

func (c *Client) GetOrder(ctx context.Context, input application.GetOrderInput) (orderdomain.Order, error) {
	response, err := c.client.GetOrder(ctx, &orderv1.GetOrderRequest{
		OrderId: input.OrderID,
		UserId:  input.UserID,
	})
	if err != nil {
		return orderdomain.Order{}, mapStatusError(err)
	}

	return mapOrder(response.GetOrder()), nil
}

func mapStatusError(err error) error {
	switch status.Code(err) {
	case codes.AlreadyExists:
		return application.ErrIdempotencyConflict
	case codes.NotFound:
		return application.ErrOrderNotFound
	case codes.InvalidArgument:
		return application.ErrInvalidCreateOrder
	default:
		return err
	}
}

func mapCreateOrderItems(items []application.RequestedItem) []*orderv1.CreateOrderItem {
	result := make([]*orderv1.CreateOrderItem, 0, len(items))
	for _, item := range items {
		result = append(result, &orderv1.CreateOrderItem{
			MenuItemId:      item.MenuItemID,
			Quantity:        item.Quantity,
			ModifierItemIds: append([]string(nil), item.ModifierItemIDs...),
		})
	}
	return result
}

func mapOrder(order *orderv1.Order) orderdomain.Order {
	if order == nil {
		return orderdomain.Order{}
	}

	items := make([]orderdomain.OrderItem, 0, len(order.GetItems()))
	for _, item := range order.GetItems() {
		items = append(items, orderdomain.OrderItem{
			MenuItemID:      item.GetMenuItemId(),
			Name:            item.GetName(),
			UnitPriceCents:  item.GetUnitPriceCents(),
			Quantity:        item.GetQuantity(),
			ModifierItemIDs: append([]string(nil), item.GetModifierItemIds()...),
			LineTotalCents:  item.GetLineTotalCents(),
		})
	}

	return orderdomain.Order{
		ID:           order.GetId(),
		UserID:       order.GetUserId(),
		RestaurantID: order.GetRestaurantId(),
		Status:       mapStatus(order.GetStatus()),
		Items:        items,
		TotalCents:   order.GetTotalCents(),
		Version:      int(order.GetVersion()),
	}
}

func mapStatus(status orderv1.OrderStatus) orderdomain.Status {
	switch status {
	case orderv1.OrderStatus_ORDER_STATUS_PENDING_RESTAURANT_CONFIRMATION:
		return orderdomain.StatusPendingRestaurantConfirmation
	case orderv1.OrderStatus_ORDER_STATUS_ACCEPTED:
		return orderdomain.StatusAccepted
	case orderv1.OrderStatus_ORDER_STATUS_REJECTED:
		return orderdomain.StatusRejected
	case orderv1.OrderStatus_ORDER_STATUS_PREPARING:
		return orderdomain.StatusPreparing
	case orderv1.OrderStatus_ORDER_STATUS_READY:
		return orderdomain.StatusReady
	case orderv1.OrderStatus_ORDER_STATUS_COMPLETED:
		return orderdomain.StatusCompleted
	case orderv1.OrderStatus_ORDER_STATUS_CANCELLED:
		return orderdomain.StatusCancelled
	default:
		return ""
	}
}
