package httpapi

import (
	"errors"
	"net/http"
	"strings"

	catalogdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/domain"
	orderapp "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/application"
)

const (
	minIdempotencyKeyLength = 8
	maxIdempotencyKeyLength = 128
)

func (h *Handler) createOrder(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User-ID")
	idempotencyKey := r.Header.Get("Idempotency-Key")
	if userID == "" || len(idempotencyKey) < minIdempotencyKeyLength || len(idempotencyKey) > maxIdempotencyKeyLength {
		writeError(w, http.StatusBadRequest, "bad_request", "X-User-ID and Idempotency-Key headers are required")
		return
	}

	var request createOrderRequest
	if err := decodeJSONBody(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON request body")
		return
	}

	result, err := h.orders.CreateOrder(r.Context(), orderapp.CreateOrderInput{
		RequestID:      r.Header.Get("X-Request-ID"),
		UserID:         userID,
		IdempotencyKey: idempotencyKey,
		RestaurantID:   request.RestaurantID,
		Items:          mapRequestedItems(request.Items),
	})
	if err != nil {
		switch {
		case errors.Is(err, orderapp.ErrIdempotencyConflict):
			writeError(w, http.StatusConflict, "idempotency_conflict", "idempotency key was reused with a different payload")
		case errors.Is(err, orderapp.ErrInvalidCreateOrder),
			errors.Is(err, catalogdomain.ErrRestaurantClosed),
			errors.Is(err, catalogdomain.ErrRestaurantNotFound),
			errors.Is(err, catalogdomain.ErrInvalidMenuRequest),
			errors.Is(err, catalogdomain.ErrMenuItemNotFound),
			errors.Is(err, catalogdomain.ErrMenuItemUnavailable),
			errors.Is(err, catalogdomain.ErrMenuModifierNotFound):
			writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		default:
			writeError(w, http.StatusInternalServerError, "order_error", "order could not be created")
		}
		return
	}

	status := http.StatusCreated
	if result.ReusedIdempotencyKey {
		status = http.StatusOK
	}
	writeJSON(w, status, mapOrder(result.Order))
}

func (h *Handler) getOrder(w http.ResponseWriter, r *http.Request) {
	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		writeError(w, http.StatusBadRequest, "bad_request", "X-User-ID header is required")
		return
	}

	orderID := strings.TrimPrefix(r.URL.Path, "/v1/orders/")
	if orderID == "" {
		writeError(w, http.StatusNotFound, "not_found", "order not found")
		return
	}

	order, err := h.orders.GetOrder(r.Context(), orderapp.GetOrderInput{
		OrderID: orderID,
		UserID:  userID,
	})
	if err != nil {
		if errors.Is(err, orderapp.ErrOrderNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "order not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "order_error", "order could not be loaded")
		return
	}

	writeJSON(w, http.StatusOK, mapOrder(order))
}
