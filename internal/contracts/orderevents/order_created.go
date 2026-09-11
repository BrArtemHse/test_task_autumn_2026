package orderevents

const (
	OrderCreatedV1Type = "order.created.v1"
	PendingStatus      = "pending_restaurant_confirmation"
)

type OrderCreatedV1 struct {
	OrderID      string               `json:"order_id"`
	UserID       string               `json:"user_id"`
	RestaurantID string               `json:"restaurant_id"`
	Status       string               `json:"status"`
	TotalCents   int64                `json:"total_cents"`
	Items        []OrderCreatedItemV1 `json:"items"`
}

type OrderCreatedItemV1 struct {
	MenuItemID      string   `json:"menu_item_id"`
	Name            string   `json:"name"`
	UnitPriceCents  int64    `json:"unit_price_cents"`
	Quantity        int32    `json:"quantity"`
	ModifierItemIDs []string `json:"modifier_item_ids"`
	LineTotalCents  int64    `json:"line_total_cents"`
}
