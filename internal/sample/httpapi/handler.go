package httpapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/contracts/orderevents"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/contracts/partnercallbacks"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/contracts/partnerevents"
)

const maxOrderBodyBytes = 1 << 20
const maxDecisionBodyBytes = 4 << 10

var ErrInvalidConfig = errors.New("invalid sample restaurant config")

type Handler struct {
	mu                  sync.RWMutex
	orders              map[string]storedOrder
	mux                 *http.ServeMux
	sender              StatusCallbackSender
	newID               func() string
	now                 func() time.Time
	operatorTokenDigest [sha256.Size]byte
}

type storedOrder struct {
	Order                orderevents.OrderCreatedV1 `json:"order"`
	ExternalStoreID      string                     `json:"external_store_id"`
	SubmissionID         string                     `json:"submission_id"`
	StatusCallbackID     string                     `json:"status_callback_id,omitempty"`
	CallbackAcknowledged bool                       `json:"callback_acknowledged"`
	callbackOccurredAt   time.Time
	callbackInFlight     bool
	fingerprint          string
}

type Config struct {
	Sender        StatusCallbackSender
	NewID         func() string
	Now           func() time.Time
	OperatorToken string
}

type StatusCallbackSender interface {
	Send(context.Context, partnercallbacks.OrderStatusV1) error
}

func NewHandler() *Handler {
	return NewHandlerWithConfig(Config{})
}

func NewHandlerWithConfig(config Config) *Handler {
	newID := config.NewID
	if newID == nil {
		newID = func() string { return "" }
	}
	now := config.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	handler := &Handler{
		orders:              make(map[string]storedOrder),
		mux:                 http.NewServeMux(),
		sender:              config.Sender,
		newID:               newID,
		now:                 now,
		operatorTokenDigest: sha256.Sum256([]byte(config.OperatorToken)),
	}
	handler.mux.HandleFunc("POST /orders", handler.receiveOrder)
	handler.mux.HandleFunc("GET /orders/{orderID}", handler.requireOperator(handler.getOrder))
	handler.mux.HandleFunc("POST /orders/{orderID}/status", handler.requireOperator(handler.changeOrderStatus))
	return handler
}

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	h.mux.ServeHTTP(writer, request)
}

func (h *Handler) requireOperator(next http.HandlerFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		token, ok := bearerToken(request.Header.Get("Authorization"))
		tokenDigest := sha256.Sum256([]byte(token))
		if !ok || subtle.ConstantTimeCompare(tokenDigest[:], h.operatorTokenDigest[:]) != 1 {
			writeError(writer, http.StatusUnauthorized, "unauthorized")
			return
		}
		next(writer, request)
	}
}

func bearerToken(authorization string) (string, bool) {
	const prefix = "Bearer "
	if !strings.HasPrefix(authorization, prefix) {
		return "", false
	}
	token := strings.TrimPrefix(authorization, prefix)
	if token == "" || strings.TrimSpace(token) != token || strings.ContainsAny(token, " \t\r\n") {
		return "", false
	}
	return token, true
}

func (h *Handler) receiveOrder(writer http.ResponseWriter, request *http.Request) {
	idempotencyKey := request.Header.Get("Idempotency-Key")
	externalStoreID := request.Header.Get("X-External-Store-ID")
	submissionID := request.Header.Get("X-Submission-ID")
	if idempotencyKey == "" || externalStoreID == "" || submissionID == "" {
		writeError(writer, http.StatusBadRequest, "required delivery headers are missing")
		return
	}

	var order orderevents.OrderCreatedV1
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, maxOrderBodyBytes))
	if err := decoder.Decode(&order); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid order payload")
		return
	}
	if order.OrderID == "" || order.OrderID != idempotencyKey || order.RestaurantID == "" || len(order.Items) == 0 {
		writeError(writer, http.StatusBadRequest, "invalid order identity")
		return
	}
	for index := range order.Items {
		if order.Items[index].ModifierItemIDs == nil {
			order.Items[index].ModifierItemIDs = []string{}
		}
	}
	canonical, err := json.Marshal(order)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, "cannot encode order")
		return
	}
	sum := sha256.Sum256(canonical)
	fingerprint := hex.EncodeToString(sum[:])

	h.mu.Lock()
	existing, found := h.orders[order.OrderID]
	if found {
		h.mu.Unlock()
		if existing.fingerprint != fingerprint {
			writeError(writer, http.StatusConflict, "order id reused with different payload")
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"order_id": order.OrderID, "duplicate": true})
		return
	}
	h.orders[order.OrderID] = storedOrder{
		Order:           order,
		ExternalStoreID: externalStoreID,
		SubmissionID:    submissionID,
		fingerprint:     fingerprint,
	}
	h.mu.Unlock()

	writeJSON(writer, http.StatusAccepted, map[string]any{"order_id": order.OrderID, "duplicate": false})
}

func (h *Handler) getOrder(writer http.ResponseWriter, request *http.Request) {
	orderID := request.PathValue("orderID")
	h.mu.RLock()
	order, found := h.orders[orderID]
	h.mu.RUnlock()
	if !found {
		writeError(writer, http.StatusNotFound, "order not found")
		return
	}
	writeJSON(writer, http.StatusOK, order)
}

type decisionRequest struct {
	Status string `json:"status"`
}

func (h *Handler) changeOrderStatus(writer http.ResponseWriter, request *http.Request) {
	var body decisionRequest
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, maxDecisionBodyBytes))
	if err := decoder.Decode(&body); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid status payload")
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		writeError(writer, http.StatusBadRequest, "invalid status payload")
		return
	}
	if body.Status != partnerevents.AcceptedStatus && body.Status != partnerevents.RejectedStatus {
		writeError(writer, http.StatusBadRequest, "unsupported status")
		return
	}

	orderID := request.PathValue("orderID")
	h.mu.Lock()
	order, found := h.orders[orderID]
	if !found {
		h.mu.Unlock()
		writeError(writer, http.StatusNotFound, "order not found")
		return
	}

	existingDecision := order.Order.Status != orderevents.PendingStatus
	if existingDecision && order.Order.Status != body.Status {
		h.mu.Unlock()
		writeError(writer, http.StatusConflict, "order decision conflict")
		return
	}
	if !existingDecision {
		callbackID := h.newID()
		if callbackID == "" {
			h.mu.Unlock()
			writeError(writer, http.StatusInternalServerError, "cannot create callback identity")
			return
		}
		order.Order.Status = body.Status
		order.StatusCallbackID = callbackID
		order.callbackOccurredAt = h.now().UTC()
	}
	if order.CallbackAcknowledged {
		h.mu.Unlock()
		writeJSON(writer, http.StatusOK, map[string]any{
			"callback_id": order.StatusCallbackID,
			"duplicate":   true,
		})
		return
	}
	if order.callbackInFlight {
		h.mu.Unlock()
		writeError(writer, http.StatusConflict, "callback already in progress")
		return
	}
	if h.sender == nil {
		h.orders[orderID] = order
		h.mu.Unlock()
		writeError(writer, http.StatusServiceUnavailable, "callback sender unavailable")
		return
	}

	order.callbackInFlight = true
	h.orders[orderID] = order
	statusCallback := partnercallbacks.OrderStatusV1{
		CallbackID:      order.StatusCallbackID,
		OrderID:         order.Order.OrderID,
		RestaurantID:    order.Order.RestaurantID,
		ExternalStoreID: order.ExternalStoreID,
		Status:          order.Order.Status,
		OccurredAt:      order.callbackOccurredAt,
	}
	h.mu.Unlock()

	sendErr := h.sender.Send(request.Context(), statusCallback)

	h.mu.Lock()
	current := h.orders[orderID]
	if current.StatusCallbackID == statusCallback.CallbackID {
		current.callbackInFlight = false
		if sendErr == nil {
			current.CallbackAcknowledged = true
		}
		h.orders[orderID] = current
	}
	h.mu.Unlock()

	if sendErr != nil {
		writeError(writer, http.StatusBadGateway, "partner callback failed")
		return
	}
	status := http.StatusAccepted
	if existingDecision {
		status = http.StatusOK
	}
	writeJSON(writer, status, map[string]any{
		"callback_id": statusCallback.CallbackID,
		"duplicate":   existingDecision,
	})
}

func writeError(writer http.ResponseWriter, status int, message string) {
	writeJSON(writer, status, map[string]string{"error": message})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
