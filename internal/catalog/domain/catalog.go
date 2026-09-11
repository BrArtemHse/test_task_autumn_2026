package catalogdomain

import "errors"

var (
	ErrRestaurantNotFound   = errors.New("restaurant not found")
	ErrRestaurantClosed     = errors.New("restaurant closed")
	ErrInvalidMenuRequest   = errors.New("invalid menu request")
	ErrMenuItemNotFound     = errors.New("menu item not found")
	ErrMenuItemUnavailable  = errors.New("menu item unavailable")
	ErrMenuModifierNotFound = errors.New("menu modifier not found")
)

type Restaurant struct {
	ID      string
	Name    string
	Cuisine string
	IsOpen  bool
}

type Menu struct {
	RestaurantID string
	Categories   []MenuCategory
}

type MenuCategory struct {
	ID    string
	Name  string
	Items []MenuItem
}

type MenuItem struct {
	ID          string
	Name        string
	Description string
	PriceCents  int64
	Available   bool
	Modifiers   []MenuModifier
}

type MenuModifier struct {
	ID              string
	Name            string
	PriceDeltaCents int64
	Available       bool
}

type RequestedItem struct {
	MenuItemID      string
	Quantity        int32
	ModifierItemIDs []string
}

type ValidatedItem struct {
	MenuItemID      string
	Name            string
	UnitPriceCents  int64
	Quantity        int32
	ModifierItemIDs []string
	LineTotalCents  int64
}
