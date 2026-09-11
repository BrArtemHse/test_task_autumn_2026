package httpapi

import (
	"context"
	"net/http"
	"strings"

	catalogdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/domain"
	orderapp "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/application"
	orderdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/domain"
)

const maxJSONBodyBytes = 1 << 20

type Dependencies struct {
	Catalog Catalog
	Orders  Orders
}

type Catalog interface {
	ListRestaurants(ctx context.Context) ([]catalogdomain.Restaurant, error)
	GetMenu(ctx context.Context, restaurantID string) (catalogdomain.Menu, error)
}

type Orders interface {
	CreateOrder(ctx context.Context, input orderapp.CreateOrderInput) (orderapp.CreateOrderResult, error)
	GetOrder(ctx context.Context, input orderapp.GetOrderInput) (orderdomain.Order, error)
}

type Handler struct {
	catalog Catalog
	orders  Orders
}

func NewHandler(deps Dependencies) http.Handler {
	return &Handler{
		catalog: deps.Catalog,
		orders:  deps.Orders,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/healthz":
		writeJSON(w, http.StatusOK, struct {
			Service string `json:"service"`
			Status  string `json:"status"`
		}{
			Service: "kitchen-api",
			Status:  "ok",
		})
	case r.Method == http.MethodGet && r.URL.Path == "/v1/restaurants":
		h.listRestaurants(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/restaurants/") && strings.HasSuffix(r.URL.Path, "/menu"):
		h.getRestaurantMenu(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/orders":
		h.createOrder(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/orders/"):
		h.getOrder(w, r)
	default:
		writeError(w, http.StatusNotFound, "not_found", "resource not found")
	}
}
