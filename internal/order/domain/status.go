package orderdomain

// Status is the durable business status stored by order-service.
type Status string

const (
	StatusPendingRestaurantConfirmation Status = "pending_restaurant_confirmation"
	StatusAccepted                      Status = "accepted"
	StatusRejected                      Status = "rejected"
	StatusPreparing                     Status = "preparing"
	StatusReady                         Status = "ready"
	StatusCompleted                     Status = "completed"
	StatusCancelled                     Status = "cancelled"
)

func canTransition(from, to Status) bool {
	allowed := map[Status][]Status{
		StatusPendingRestaurantConfirmation: {
			StatusAccepted,
			StatusRejected,
			StatusCancelled,
		},
		StatusAccepted: {
			StatusPreparing,
			StatusCancelled,
		},
		StatusPreparing: {
			StatusReady,
			StatusCancelled,
		},
		StatusReady: {
			StatusCompleted,
		},
	}

	for _, candidate := range allowed[from] {
		if candidate == to {
			return true
		}
	}

	return false
}
