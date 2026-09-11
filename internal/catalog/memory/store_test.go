package memory

import (
	"context"
	"errors"
	"testing"

	catalogdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/domain"
)

func TestFixtureStoreListsRestaurants(t *testing.T) {
	store := NewFixtureStore()

	restaurants, err := store.ListRestaurants(context.Background())
	if err != nil {
		t.Fatalf("ListRestaurants() error = %v", err)
	}
	if len(restaurants) != 1 {
		t.Fatalf("restaurants = %d, want 1", len(restaurants))
	}
	if restaurants[0].ID != "rst-pizza-1" {
		t.Fatalf("restaurant id = %q, want rst-pizza-1", restaurants[0].ID)
	}
}

func TestFixtureStoreGetsRestaurantMenu(t *testing.T) {
	store := NewFixtureStore()

	menu, err := store.GetMenu(context.Background(), "rst-pizza-1")
	if err != nil {
		t.Fatalf("GetMenu() error = %v", err)
	}
	if menu.RestaurantID != "rst-pizza-1" {
		t.Fatalf("restaurant id = %q, want rst-pizza-1", menu.RestaurantID)
	}
	if len(menu.Categories) == 0 || len(menu.Categories[0].Items) == 0 {
		t.Fatalf("menu categories/items are empty: %#v", menu)
	}
}

func TestFixtureStoreValidatesOrderItems(t *testing.T) {
	store := NewFixtureStore()

	items, err := store.ValidateOrderItems(context.Background(), "rst-pizza-1", []catalogdomain.RequestedItem{
		{MenuItemID: "item-margherita", Quantity: 2, ModifierItemIDs: []string{"mod-extra-cheese"}},
	})
	if err != nil {
		t.Fatalf("ValidateOrderItems() error = %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("items = %d, want 1", len(items))
	}
	if items[0].Name != "Margherita" {
		t.Fatalf("item name = %q, want Margherita", items[0].Name)
	}
	if items[0].UnitPriceCents != 69000 {
		t.Fatalf("unit price = %d, want 69000", items[0].UnitPriceCents)
	}
	if items[0].LineTotalCents != 138000 {
		t.Fatalf("line total = %d, want 138000", items[0].LineTotalCents)
	}
}

func TestFixtureStoreRejectsUnavailableItem(t *testing.T) {
	store := NewFixtureStore()

	_, err := store.ValidateOrderItems(context.Background(), "rst-pizza-1", []catalogdomain.RequestedItem{
		{MenuItemID: "item-lasagna", Quantity: 1},
	})
	if !errors.Is(err, catalogdomain.ErrMenuItemUnavailable) {
		t.Fatalf("ValidateOrderItems() error = %v, want %v", err, catalogdomain.ErrMenuItemUnavailable)
	}
}

func TestFixtureStoreRejectsClosedRestaurant(t *testing.T) {
	store := NewFixtureStore()
	store.restaurants[0].IsOpen = false

	if _, err := store.GetMenu(context.Background(), "rst-pizza-1"); err != nil {
		t.Fatalf("closed restaurant menu should remain readable: %v", err)
	}
	_, err := store.ValidateOrderItems(context.Background(), "rst-pizza-1", []catalogdomain.RequestedItem{
		{MenuItemID: "item-margherita", Quantity: 1},
	})
	if !errors.Is(err, catalogdomain.ErrRestaurantClosed) {
		t.Fatalf("ValidateOrderItems() error = %v, want restaurant closed", err)
	}
}

func TestFixtureStoreRejectsMissingRestaurant(t *testing.T) {
	store := NewFixtureStore()

	_, err := store.GetMenu(context.Background(), "unknown")
	if !errors.Is(err, catalogdomain.ErrRestaurantNotFound) {
		t.Fatalf("GetMenu() error = %v, want %v", err, catalogdomain.ErrRestaurantNotFound)
	}
}
