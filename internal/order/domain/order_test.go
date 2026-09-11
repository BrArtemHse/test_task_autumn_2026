package orderdomain

import (
	"errors"
	"testing"
)

func TestNewOrderBuildsPriceSnapshot(t *testing.T) {
	order, err := NewOrder(NewOrderInput{
		OrderID:      "ord-1",
		UserID:       "usr-1",
		RestaurantID: "rst-1",
		Items: []NewOrderItemInput{
			{MenuItemID: "pizza", Name: "Pizza", UnitPriceCents: 1200, Quantity: 2},
			{MenuItemID: "tea", Name: "Tea", UnitPriceCents: 250, Quantity: 1},
		},
	})
	if err != nil {
		t.Fatalf("NewOrder() error = %v", err)
	}

	if order.Status != StatusPendingRestaurantConfirmation {
		t.Fatalf("status = %q, want %q", order.Status, StatusPendingRestaurantConfirmation)
	}
	if order.TotalCents != 2650 {
		t.Fatalf("total = %d, want 2650", order.TotalCents)
	}
	if order.Version != 1 {
		t.Fatalf("version = %d, want 1", order.Version)
	}
	if got := order.Items[0].UnitPriceCents; got != 1200 {
		t.Fatalf("first item price snapshot = %d, want 1200", got)
	}
}

func TestNewOrderRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name  string
		input NewOrderInput
		want  error
	}{
		{
			name: "missing order id",
			input: NewOrderInput{
				UserID:       "usr-1",
				RestaurantID: "rst-1",
				Items:        []NewOrderItemInput{{MenuItemID: "pizza", Name: "Pizza", UnitPriceCents: 1200, Quantity: 1}},
			},
			want: ErrInvalidOrder,
		},
		{
			name: "empty items",
			input: NewOrderInput{
				OrderID:      "ord-1",
				UserID:       "usr-1",
				RestaurantID: "rst-1",
			},
			want: ErrEmptyOrder,
		},
		{
			name: "non-positive quantity",
			input: NewOrderInput{
				OrderID:      "ord-1",
				UserID:       "usr-1",
				RestaurantID: "rst-1",
				Items:        []NewOrderItemInput{{MenuItemID: "pizza", Name: "Pizza", UnitPriceCents: 1200, Quantity: 0}},
			},
			want: ErrInvalidOrderItem,
		},
		{
			name: "non-positive price",
			input: NewOrderInput{
				OrderID:      "ord-1",
				UserID:       "usr-1",
				RestaurantID: "rst-1",
				Items:        []NewOrderItemInput{{MenuItemID: "pizza", Name: "Pizza", UnitPriceCents: 0, Quantity: 1}},
			},
			want: ErrInvalidOrderItem,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewOrder(tt.input)
			if !errors.Is(err, tt.want) {
				t.Fatalf("NewOrder() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestOrderTransitionToAppliesAllowedTransitions(t *testing.T) {
	order := mustOrder(t)

	steps := []Status{
		StatusAccepted,
		StatusPreparing,
		StatusReady,
		StatusCompleted,
	}

	for version, next := range steps {
		changed, err := order.TransitionTo(next)
		if err != nil {
			t.Fatalf("TransitionTo(%q) error = %v", next, err)
		}
		if !changed {
			t.Fatalf("TransitionTo(%q) changed = false, want true", next)
		}
		if order.Status != next {
			t.Fatalf("status = %q, want %q", order.Status, next)
		}
		if order.Version != version+2 {
			t.Fatalf("version = %d, want %d", order.Version, version+2)
		}
	}
}

func TestOrderTransitionToRejectsInvalidTransitions(t *testing.T) {
	order := mustOrder(t)

	changed, err := order.TransitionTo(StatusPreparing)
	if !errors.Is(err, ErrInvalidStatusTransition) {
		t.Fatalf("TransitionTo(preparing) error = %v, want %v", err, ErrInvalidStatusTransition)
	}
	if changed {
		t.Fatal("TransitionTo(preparing) changed = true, want false")
	}
	if order.Status != StatusPendingRestaurantConfirmation {
		t.Fatalf("status = %q, want %q", order.Status, StatusPendingRestaurantConfirmation)
	}
	if order.Version != 1 {
		t.Fatalf("version = %d, want 1", order.Version)
	}
}

func TestOrderTransitionToTreatsDuplicateStatusAsNoop(t *testing.T) {
	order := mustOrder(t)

	changed, err := order.TransitionTo(StatusPendingRestaurantConfirmation)
	if err != nil {
		t.Fatalf("TransitionTo(current) error = %v", err)
	}
	if changed {
		t.Fatal("TransitionTo(current) changed = true, want false")
	}
	if order.Version != 1 {
		t.Fatalf("version = %d, want 1", order.Version)
	}
}

func TestFingerprintCreateOrderIsStableForEquivalentItems(t *testing.T) {
	left := NewOrderInput{
		OrderID:      "ord-1",
		UserID:       "usr-1",
		RestaurantID: "rst-1",
		Items: []NewOrderItemInput{
			{MenuItemID: "tea", Name: "Tea", UnitPriceCents: 250, Quantity: 1},
			{MenuItemID: "pizza", Name: "Pizza", UnitPriceCents: 1200, Quantity: 2},
		},
	}
	right := NewOrderInput{
		OrderID:      "ord-1",
		UserID:       "usr-1",
		RestaurantID: "rst-1",
		Items: []NewOrderItemInput{
			{MenuItemID: "pizza", Name: "Pizza", UnitPriceCents: 1200, Quantity: 2},
			{MenuItemID: "tea", Name: "Tea", UnitPriceCents: 250, Quantity: 1},
		},
	}

	if FingerprintCreateOrder(left) != FingerprintCreateOrder(right) {
		t.Fatal("equivalent create-order payloads produced different fingerprints")
	}
}

func TestFingerprintCreateOrderChangesWhenSemanticPayloadChanges(t *testing.T) {
	base := NewOrderInput{
		OrderID:      "ord-1",
		UserID:       "usr-1",
		RestaurantID: "rst-1",
		Items:        []NewOrderItemInput{{MenuItemID: "pizza", Name: "Pizza", UnitPriceCents: 1200, Quantity: 2}},
	}
	changed := NewOrderInput{
		OrderID:      "ord-1",
		UserID:       "usr-1",
		RestaurantID: "rst-1",
		Items:        []NewOrderItemInput{{MenuItemID: "pizza", Name: "Pizza", UnitPriceCents: 1200, Quantity: 3}},
	}

	if FingerprintCreateOrder(base) == FingerprintCreateOrder(changed) {
		t.Fatal("different create-order payloads produced the same fingerprint")
	}
}

func mustOrder(t *testing.T) *Order {
	t.Helper()

	order, err := NewOrder(NewOrderInput{
		OrderID:      "ord-1",
		UserID:       "usr-1",
		RestaurantID: "rst-1",
		Items:        []NewOrderItemInput{{MenuItemID: "pizza", Name: "Pizza", UnitPriceCents: 1200, Quantity: 1}},
	})
	if err != nil {
		t.Fatalf("NewOrder() error = %v", err)
	}

	return &order
}
