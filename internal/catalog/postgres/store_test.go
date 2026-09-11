package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	catalogdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/domain"
)

func TestStoreListsRestaurants(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery("SELECT id, name, cuisine, is_open").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "cuisine", "is_open"}).
			AddRow("rst-1", "Pizza Roma", "Italian", true).
			AddRow("rst-2", "Green Bowl", "Healthy", false))

	restaurants, err := NewStore(db).ListRestaurants(context.Background())
	if err != nil {
		t.Fatalf("ListRestaurants() error = %v", err)
	}
	if len(restaurants) != 2 {
		t.Fatalf("restaurants = %d, want 2", len(restaurants))
	}
	if restaurants[0].ID != "rst-1" || restaurants[1].ID != "rst-2" {
		t.Fatalf("restaurant order = %#v", restaurants)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestStoreBuildsRestaurantMenu(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	expectMenu(mock, "rst-1")

	menu, err := NewStore(db).GetMenu(context.Background(), "rst-1")
	if err != nil {
		t.Fatalf("GetMenu() error = %v", err)
	}
	if menu.RestaurantID != "rst-1" || len(menu.Categories) != 2 {
		t.Fatalf("menu = %#v", menu)
	}
	pizza := menu.Categories[0].Items[0]
	if pizza.ID != "item-margherita" || !pizza.Available || len(pizza.Modifiers) != 1 {
		t.Fatalf("pizza = %#v", pizza)
	}
	if pizza.Modifiers[0].PriceDeltaCents != 10000 {
		t.Fatalf("modifier price = %d, want 10000", pizza.Modifiers[0].PriceDeltaCents)
	}
	lemonade := menu.Categories[1].Items[0]
	if lemonade.ID != "item-lemonade" || lemonade.Available {
		t.Fatalf("lemonade = %#v", lemonade)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestStoreReturnsRestaurantNotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery("SELECT EXISTS").
		WithArgs("missing").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	_, err = NewStore(db).GetMenu(context.Background(), "missing")
	if !errors.Is(err, catalogdomain.ErrRestaurantNotFound) {
		t.Fatalf("GetMenu() error = %v, want %v", err, catalogdomain.ErrRestaurantNotFound)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestStoreValidatesItemsAgainstMenuSnapshot(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	expectOpenRestaurant(mock, "rst-1")
	expectMenu(mock, "rst-1")

	items, err := NewStore(db).ValidateOrderItems(context.Background(), "rst-1", []catalogdomain.RequestedItem{
		{MenuItemID: "item-margherita", Quantity: 2, ModifierItemIDs: []string{"mod-extra-cheese"}},
	})
	if err != nil {
		t.Fatalf("ValidateOrderItems() error = %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("items = %d, want 1", len(items))
	}
	if items[0].UnitPriceCents != 79000 || items[0].LineTotalCents != 158000 {
		t.Fatalf("validated item = %#v", items[0])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestStoreRejectsUnavailableItem(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	expectOpenRestaurant(mock, "rst-1")
	expectMenu(mock, "rst-1")

	_, err = NewStore(db).ValidateOrderItems(context.Background(), "rst-1", []catalogdomain.RequestedItem{
		{MenuItemID: "item-lemonade", Quantity: 1},
	})
	if !errors.Is(err, catalogdomain.ErrMenuItemUnavailable) {
		t.Fatalf("ValidateOrderItems() error = %v, want %v", err, catalogdomain.ErrMenuItemUnavailable)
	}
}

func TestStoreRejectsClosedRestaurant(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery("SELECT is_open FROM catalog.restaurants").
		WithArgs("rst-1").
		WillReturnRows(sqlmock.NewRows([]string{"is_open"}).AddRow(false))

	_, err = NewStore(db).ValidateOrderItems(context.Background(), "rst-1", []catalogdomain.RequestedItem{
		{MenuItemID: "item-margherita", Quantity: 1},
	})
	if !errors.Is(err, catalogdomain.ErrRestaurantClosed) {
		t.Fatalf("ValidateOrderItems() error = %v, want restaurant closed", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreValidateOrderItemsHandlesRestaurantLookupErrors(t *testing.T) {
	dbError := errors.New("database offline")
	for _, tt := range []struct {
		name       string
		queryError error
		wantError  error
	}{
		{name: "missing", queryError: sql.ErrNoRows, wantError: catalogdomain.ErrRestaurantNotFound},
		{name: "database error", queryError: dbError, wantError: dbError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			mock.ExpectQuery("SELECT is_open FROM catalog.restaurants").
				WithArgs("rst-1").WillReturnError(tt.queryError)
			_, err = NewStore(db).ValidateOrderItems(context.Background(), "rst-1", []catalogdomain.RequestedItem{
				{MenuItemID: "item-margherita", Quantity: 1},
			})
			if !errors.Is(err, tt.wantError) {
				t.Fatalf("error = %v, want %v", err, tt.wantError)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func expectOpenRestaurant(mock sqlmock.Sqlmock, restaurantID string) {
	mock.ExpectQuery("SELECT is_open FROM catalog.restaurants").
		WithArgs(restaurantID).
		WillReturnRows(sqlmock.NewRows([]string{"is_open"}).AddRow(true))
}

func expectMenu(mock sqlmock.Sqlmock, restaurantID string) {
	mock.ExpectQuery("SELECT EXISTS").
		WithArgs(restaurantID).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery("(?s)SELECT.*FROM catalog\\.menu_categories").
		WithArgs(restaurantID).
		WillReturnRows(sqlmock.NewRows([]string{
			"category_id",
			"category_name",
			"item_id",
			"item_name",
			"description",
			"price_cents",
			"available",
			"modifier_id",
			"modifier_name",
			"price_delta_cents",
			"modifier_available",
		}).
			AddRow("cat-pizza", "Pizza", "item-margherita", "Margherita", "Tomato and cheese", int64(69000), true, "mod-extra-cheese", "Extra cheese", int64(10000), true).
			AddRow("cat-drinks", "Drinks", "item-lemonade", "Lemonade", "House lemonade", int64(22000), false, nil, nil, nil, nil))
}
