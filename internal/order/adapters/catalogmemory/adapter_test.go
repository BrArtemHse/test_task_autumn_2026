package catalogmemory

import (
	"context"
	"testing"

	catalogmemory "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/memory"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/application"
)

func TestAdapterValidatesItemsThroughCatalogStore(t *testing.T) {
	adapter := New(catalogmemory.NewFixtureStore())

	items, err := adapter.ValidateOrderItems(context.Background(), "rst-pizza-1", []application.RequestedItem{
		{MenuItemID: "item-margherita", Quantity: 2, ModifierItemIDs: []string{"mod-extra-cheese"}},
	})
	if err != nil {
		t.Fatalf("ValidateOrderItems() error = %v", err)
	}

	if len(items) != 1 {
		t.Fatalf("items = %d, want 1", len(items))
	}
	if items[0].MenuItemID != "item-margherita" {
		t.Fatalf("menu item id = %q, want item-margherita", items[0].MenuItemID)
	}
	if items[0].Name != "Margherita" {
		t.Fatalf("name = %q, want Margherita", items[0].Name)
	}
	if items[0].UnitPriceCents != 69000 {
		t.Fatalf("unit price = %d, want 69000", items[0].UnitPriceCents)
	}
}
