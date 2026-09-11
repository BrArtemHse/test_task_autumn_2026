package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/contracts/partnercallbacks"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/callback"
)

const maxCallbackBodyBytes = 64 << 10

type StatusCallbackService interface {
	ReceiveStatusCallback(context.Context, callback.ReceiveStatusCallbackInput) (callback.ReceiveResult, error)
}

type Handler struct {
	service StatusCallbackService
	mux     *http.ServeMux
}

func NewHandler(service StatusCallbackService) *Handler {
	handler := &Handler{
		service: service,
		mux:     http.NewServeMux(),
	}
	handler.mux.HandleFunc("POST /callbacks/order-status", handler.receiveStatusCallback)
	return handler
}

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	h.mux.ServeHTTP(writer, request)
}

func (h *Handler) receiveStatusCallback(writer http.ResponseWriter, request *http.Request) {
	token, ok := bearerToken(request.Header.Get("Authorization"))
	if !ok {
		writeError(writer, http.StatusUnauthorized, "unauthorized")
		return
	}

	var body partnercallbacks.OrderStatusV1
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, maxCallbackBodyBytes))
	if err := decoder.Decode(&body); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid callback payload")
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		writeError(writer, http.StatusBadRequest, "invalid callback payload")
		return
	}
	if h.service == nil {
		writeError(writer, http.StatusInternalServerError, "callback service unavailable")
		return
	}

	result, err := h.service.ReceiveStatusCallback(request.Context(), callback.ReceiveStatusCallbackInput{
		RequestID:         request.Header.Get("X-Request-ID"),
		BearerToken:       token,
		CallbackID:        body.CallbackID,
		OrderID:           body.OrderID,
		RestaurantID:      body.RestaurantID,
		ExternalStoreID:   body.ExternalStoreID,
		Status:            body.Status,
		PartnerOccurredAt: body.OccurredAt,
	})
	if err != nil {
		writeCallbackError(writer, err)
		return
	}
	status := http.StatusAccepted
	if result.Duplicate {
		status = http.StatusOK
	}
	writeJSON(writer, status, map[string]any{
		"callback_id": body.CallbackID,
		"duplicate":   result.Duplicate,
	})
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

func writeCallbackError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, callback.ErrInvalidStatusCallback):
		writeError(writer, http.StatusBadRequest, "invalid callback")
	case errors.Is(err, callback.ErrUnauthorizedCallback):
		writeError(writer, http.StatusUnauthorized, "unauthorized")
	case errors.Is(err, callback.ErrCallbackConflict):
		writeError(writer, http.StatusConflict, "callback identity conflict")
	default:
		writeError(writer, http.StatusInternalServerError, "callback processing failed")
	}
}

func writeError(writer http.ResponseWriter, status int, message string) {
	writeJSON(writer, status, map[string]string{"error": message})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
