package partnercallbacks

import "time"

// OrderStatusV1 is the restaurant-to-partner HTTP callback contract.
type OrderStatusV1 struct {
	CallbackID      string    `json:"callback_id"`
	OrderID         string    `json:"order_id"`
	RestaurantID    string    `json:"restaurant_id"`
	ExternalStoreID string    `json:"external_store_id"`
	Status          string    `json:"status"`
	OccurredAt      time.Time `json:"occurred_at"`
}
