package partnerevents

import "time"

const (
	OrderAcceptedV1Type = "partner.order_accepted.v1"
	OrderRejectedV1Type = "partner.order_rejected.v1"

	AcceptedStatus = "accepted"
	RejectedStatus = "rejected"
)

// OrderStatusChangedV1 is the persisted partner-to-order event payload.
type OrderStatusChangedV1 struct {
	CallbackID        string    `json:"callback_id"`
	OrderID           string    `json:"order_id"`
	RestaurantID      string    `json:"restaurant_id"`
	ExternalStoreID   string    `json:"external_store_id"`
	Status            string    `json:"status"`
	PartnerOccurredAt time.Time `json:"partner_occurred_at"`
}
