package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	catalogmemory "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/memory"
	catalogadapter "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/adapters/catalogmemory"
	orderapp "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/application"
	ordermemory "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/memory"
)

func TestHandlerListsRestaurants(t *testing.T) {
	handler := newTestHandler()
	request := httptest.NewRequest(http.MethodGet, "/v1/restaurants", nil)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var body struct {
		Restaurants []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"restaurants"`
	}
	decodeJSON(t, recorder, &body)
	if len(body.Restaurants) != 1 {
		t.Fatalf("restaurants = %d, want 1", len(body.Restaurants))
	}
	if body.Restaurants[0].ID != "rst-pizza-1" {
		t.Fatalf("restaurant id = %q, want rst-pizza-1", body.Restaurants[0].ID)
	}
}

func TestHandlerHealthz(t *testing.T) {
	handler := newTestHandler()
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
}

func TestHandlerGetsRestaurantMenu(t *testing.T) {
	handler := newTestHandler()
	request := httptest.NewRequest(http.MethodGet, "/v1/restaurants/rst-pizza-1/menu", nil)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var body struct {
		RestaurantID string `json:"restaurantId"`
		Categories   []struct {
			Items []struct {
				ID        string `json:"id"`
				Available bool   `json:"available"`
			} `json:"items"`
		} `json:"categories"`
	}
	decodeJSON(t, recorder, &body)
	if body.RestaurantID != "rst-pizza-1" {
		t.Fatalf("restaurant id = %q, want rst-pizza-1", body.RestaurantID)
	}
	if body.Categories[0].Items[0].ID != "item-margherita" {
		t.Fatalf("first item id = %q, want item-margherita", body.Categories[0].Items[0].ID)
	}
}

func TestHandlerCreatesOrderAndReusesIdempotencyKey(t *testing.T) {
	handler := newTestHandler()
	body := []byte(`{"restaurantId":"rst-pizza-1","items":[{"menuItemId":"item-margherita","quantity":2}]}`)

	first := performCreateOrder(handler, "idem-key-1", body)
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d, want %d; body = %s", first.Code, http.StatusCreated, first.Body.String())
	}

	var firstOrder orderResponse
	decodeJSON(t, first, &firstOrder)
	if firstOrder.ID != "ord-1" {
		t.Fatalf("first order id = %q, want ord-1", firstOrder.ID)
	}
	if firstOrder.Status != "pending_restaurant_confirmation" {
		t.Fatalf("status = %q", firstOrder.Status)
	}
	if firstOrder.TotalCents != 138000 {
		t.Fatalf("total = %d, want 138000", firstOrder.TotalCents)
	}
	if firstOrder.Version != 1 {
		t.Fatalf("version = %d, want 1", firstOrder.Version)
	}

	second := performCreateOrder(handler, "idem-key-1", body)
	if second.Code != http.StatusOK {
		t.Fatalf("second status = %d, want %d; body = %s", second.Code, http.StatusOK, second.Body.String())
	}
	var secondOrder orderResponse
	decodeJSON(t, second, &secondOrder)
	if secondOrder.ID != "ord-1" {
		t.Fatalf("second order id = %q, want original ord-1", secondOrder.ID)
	}
}

func TestHandlerRejectsInvalidIdempotencyKeyLength(t *testing.T) {
	body := []byte("{\"restaurantId\":\"rst-pizza-1\",\"items\":[{\"menuItemId\":\"item-margherita\",\"quantity\":1}]}")

	tests := []struct {
		name string
		key  string
	}{
		{name: "too short", key: strings.Repeat("a", 7)},
		{name: "too long", key: strings.Repeat("a", 129)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := performCreateOrder(newTestHandler(), test.key, body)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body = %s", response.Code, http.StatusBadRequest, response.Body.String())
			}
		})
	}
}

func TestHandlerRejectsTrailingJSONValue(t *testing.T) {
	body := []byte("{\"restaurantId\":\"rst-pizza-1\",\"items\":[{\"menuItemId\":\"item-margherita\",\"quantity\":1}]} {}")

	response := performCreateOrder(newTestHandler(), "idem-key-1", body)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body = %s", response.Code, http.StatusBadRequest, response.Body.String())
	}
}

func TestHandlerReturnsConflictForIdempotencyPayloadMismatch(t *testing.T) {
	handler := newTestHandler()
	firstBody := []byte(`{"restaurantId":"rst-pizza-1","items":[{"menuItemId":"item-margherita","quantity":1}]}`)
	secondBody := []byte(`{"restaurantId":"rst-pizza-1","items":[{"menuItemId":"item-margherita","quantity":2}]}`)

	first := performCreateOrder(handler, "idem-key-1", firstBody)
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d, want %d; body = %s", first.Code, http.StatusCreated, first.Body.String())
	}

	second := performCreateOrder(handler, "idem-key-1", secondBody)
	if second.Code != http.StatusConflict {
		t.Fatalf("second status = %d, want %d; body = %s", second.Code, http.StatusConflict, second.Body.String())
	}
}

func TestHandlerGetsOrder(t *testing.T) {
	handler := newTestHandler()
	create := performCreateOrder(handler, "idem-key-1", []byte(`{"restaurantId":"rst-pizza-1","items":[{"menuItemId":"item-margherita","quantity":1}]}`))
	if create.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d; body = %s", create.Code, http.StatusCreated, create.Body.String())
	}

	request := httptest.NewRequest(http.MethodGet, "/v1/orders/ord-1", nil)
	request.Header.Set("X-User-ID", "usr-1")
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var body orderResponse
	decodeJSON(t, recorder, &body)
	if body.ID != "ord-1" {
		t.Fatalf("order id = %q, want ord-1", body.ID)
	}
}

func newTestHandler() http.Handler {
	catalogStore := catalogmemory.NewFixtureStore()
	orderStore := ordermemory.NewStore()
	orderService := orderapp.NewService(orderapp.ServiceConfig{
		Catalog: catalogadapter.New(catalogStore),
		Store:   orderStore,
		NewID:   sequenceID("ord-1", "evt-1", "ord-2", "evt-2", "ord-3", "evt-3"),
		Now:     func() time.Time { return time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC) },
	})
	return NewHandler(Dependencies{
		Catalog: catalogStore,
		Orders:  orderService,
	})
}

func performCreateOrder(handler http.Handler, idempotencyKey string, body []byte) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/v1/orders", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-User-ID", "usr-1")
	request.Header.Set("Idempotency-Key", idempotencyKey)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func decodeJSON(t *testing.T, recorder *httptest.ResponseRecorder, target any) {
	t.Helper()

	if err := json.Unmarshal(recorder.Body.Bytes(), target); err != nil {
		t.Fatalf("decode JSON error = %v; body = %s", err, recorder.Body.String())
	}
}

func sequenceID(values ...string) func() string {
	next := 0
	return func() string {
		value := values[next]
		next++
		return value
	}
}
