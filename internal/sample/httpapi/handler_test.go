package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/contracts/orderevents"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/contracts/partnercallbacks"
	samplehttp "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/sample/httpapi"
)

const operatorToken = "sample-operator-token"

func TestHandlerAcceptsAndReturnsOrder(t *testing.T) {
	handler := samplehttp.NewHandlerWithConfig(samplehttp.Config{OperatorToken: operatorToken})
	request := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewReader(orderPayload(69000)))
	request.Header.Set("Idempotency-Key", "ord-1")
	request.Header.Set("X-External-Store-ID", "store-pizza-1")
	request.Header.Set("X-Submission-ID", "sub-1")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("POST status = %d, want 202; body=%s", response.Code, response.Body.String())
	}

	getResponse := httptest.NewRecorder()
	handler.ServeHTTP(getResponse, operatorRequest(http.MethodGet, "/orders/ord-1", nil))
	if getResponse.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", getResponse.Code)
	}
	var body struct {
		Order           orderevents.OrderCreatedV1 `json:"order"`
		ExternalStoreID string                     `json:"external_store_id"`
		SubmissionID    string                     `json:"submission_id"`
	}
	if err := json.Unmarshal(getResponse.Body.Bytes(), &body); err != nil {
		t.Fatalf("response JSON error = %v", err)
	}
	if body.Order.OrderID != "ord-1" || body.ExternalStoreID != "store-pizza-1" || body.SubmissionID != "sub-1" {
		t.Fatalf("response = %#v", body)
	}
}

func TestHandlerTreatsSameOrderPayloadAsIdempotent(t *testing.T) {
	handler := samplehttp.NewHandler()
	for attempt, wantStatus := range []int{http.StatusAccepted, http.StatusOK} {
		request := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewReader(orderPayload(69000)))
		request.Header.Set("Idempotency-Key", "ord-1")
		request.Header.Set("X-External-Store-ID", "store-pizza-1")
		request.Header.Set("X-Submission-ID", "sub-1")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != wantStatus {
			t.Fatalf("attempt %d status = %d, want %d", attempt+1, response.Code, wantStatus)
		}
	}
}

func TestHandlerRejectsConflictingPayloadForSameOrder(t *testing.T) {
	handler := samplehttp.NewHandler()
	first := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewReader(orderPayload(69000)))
	first.Header.Set("Idempotency-Key", "ord-1")
	first.Header.Set("X-External-Store-ID", "store-pizza-1")
	first.Header.Set("X-Submission-ID", "sub-1")
	handler.ServeHTTP(httptest.NewRecorder(), first)

	conflict := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewReader(orderPayload(70000)))
	conflict.Header.Set("Idempotency-Key", "ord-1")
	conflict.Header.Set("X-External-Store-ID", "store-pizza-1")
	conflict.Header.Set("X-Submission-ID", "sub-2")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, conflict)

	if response.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d, want 409", response.Code)
	}
}

func TestHandlerAcceptsOrderAndSendsStableStatusCallback(t *testing.T) {
	sender := &callbackSenderStub{}
	handler := samplehttp.NewHandlerWithConfig(samplehttp.Config{
		Sender:        sender,
		NewID:         func() string { return "callback-1" },
		Now:           fixedSampleTime,
		OperatorToken: operatorToken,
	})
	receiveSampleOrder(t, handler)

	request := operatorRequest(http.MethodPost, "/orders/ord-1/status", strings.NewReader(`{"status":"accepted"}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", response.Code, response.Body.String())
	}
	if len(sender.callbacks) != 1 {
		t.Fatalf("callbacks = %d, want 1", len(sender.callbacks))
	}
	callback := sender.callbacks[0]
	if callback.CallbackID != "callback-1" ||
		callback.OrderID != "ord-1" ||
		callback.RestaurantID != "rst-pizza-1" ||
		callback.ExternalStoreID != "store-pizza-1" ||
		callback.Status != "accepted" ||
		!callback.OccurredAt.Equal(fixedSampleTime()) {
		t.Fatalf("callback = %#v", callback)
	}
}

func TestHandlerRetriesSameDecisionWithSameCallbackIdentity(t *testing.T) {
	sender := &callbackSenderStub{errors: []error{errors.New("partner unavailable"), nil}}
	ids := 0
	handler := samplehttp.NewHandlerWithConfig(samplehttp.Config{
		Sender:        sender,
		OperatorToken: operatorToken,
		NewID: func() string {
			ids++
			return "callback-1"
		},
		Now: fixedSampleTime,
	})
	receiveSampleOrder(t, handler)

	for attempt, wantStatus := range []int{http.StatusBadGateway, http.StatusOK} {
		request := operatorRequest(http.MethodPost, "/orders/ord-1/status", strings.NewReader(`{"status":"accepted"}`))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != wantStatus {
			t.Fatalf("attempt %d status = %d, want %d", attempt+1, response.Code, wantStatus)
		}
	}
	if ids != 1 {
		t.Fatalf("generated ids = %d, want 1", ids)
	}
	if len(sender.callbacks) != 2 ||
		sender.callbacks[0].CallbackID != sender.callbacks[1].CallbackID ||
		!sender.callbacks[0].OccurredAt.Equal(sender.callbacks[1].OccurredAt) {
		t.Fatalf("callbacks are not stable: %#v", sender.callbacks)
	}
}

func TestHandlerRejectsConflictingOrInvalidDecision(t *testing.T) {
	sender := &callbackSenderStub{}
	handler := samplehttp.NewHandlerWithConfig(samplehttp.Config{
		Sender:        sender,
		NewID:         func() string { return "callback-1" },
		Now:           fixedSampleTime,
		OperatorToken: operatorToken,
	})
	receiveSampleOrder(t, handler)

	accepted := httptest.NewRecorder()
	handler.ServeHTTP(accepted, operatorRequest(http.MethodPost, "/orders/ord-1/status", strings.NewReader(`{"status":"accepted"}`)))
	if accepted.Code != http.StatusAccepted {
		t.Fatalf("accepted status = %d, want 202", accepted.Code)
	}

	conflict := httptest.NewRecorder()
	handler.ServeHTTP(conflict, operatorRequest(http.MethodPost, "/orders/ord-1/status", strings.NewReader(`{"status":"rejected"}`)))
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d, want 409", conflict.Code)
	}

	invalid := httptest.NewRecorder()
	handler.ServeHTTP(invalid, operatorRequest(http.MethodPost, "/orders/ord-1/status", strings.NewReader(`{"status":"preparing"}`)))
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid status = %d, want 400", invalid.Code)
	}
	if len(sender.callbacks) != 1 {
		t.Fatalf("callbacks = %d, want 1", len(sender.callbacks))
	}
}

func TestHandlerReturnsNotFoundForUnknownOrderDecision(t *testing.T) {
	handler := samplehttp.NewHandlerWithConfig(samplehttp.Config{
		Sender:        &callbackSenderStub{},
		NewID:         func() string { return "callback-1" },
		Now:           fixedSampleTime,
		OperatorToken: operatorToken,
	})
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, operatorRequest(http.MethodPost, "/orders/missing/status", strings.NewReader(`{"status":"accepted"}`)))

	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", response.Code)
	}
}

func TestHandlerProtectsOperatorRoutes(t *testing.T) {
	handler := samplehttp.NewHandlerWithConfig(samplehttp.Config{OperatorToken: operatorToken})
	receiveSampleOrder(t, handler)

	tests := []struct {
		name   string
		method string
		target string
		body   string
		token  string
	}{
		{name: "read without token", method: http.MethodGet, target: "/orders/ord-1"},
		{name: "read with wrong token", method: http.MethodGet, target: "/orders/ord-1", token: "wrong"},
		{name: "decision without token", method: http.MethodPost, target: "/orders/ord-1/status", body: `{"status":"accepted"}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, test.target, strings.NewReader(test.body))
			if test.token != "" {
				request.Header.Set("Authorization", "Bearer "+test.token)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401; body=%s", response.Code, response.Body.String())
			}
		})
	}
}

type callbackSenderStub struct {
	callbacks []partnercallbacks.OrderStatusV1
	errors    []error
}

func (s *callbackSenderStub) Send(_ context.Context, statusCallback partnercallbacks.OrderStatusV1) error {
	s.callbacks = append(s.callbacks, statusCallback)
	if len(s.errors) == 0 {
		return nil
	}
	err := s.errors[0]
	s.errors = s.errors[1:]
	return err
}

func receiveSampleOrder(t *testing.T, handler http.Handler) {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewReader(orderPayload(69000)))
	request.Header.Set("Idempotency-Key", "ord-1")
	request.Header.Set("X-External-Store-ID", "store-pizza-1")
	request.Header.Set("X-Submission-ID", "sub-1")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("receive order status = %d, want 202", response.Code)
	}
}

func operatorRequest(method, target string, body io.Reader) *http.Request {
	request := httptest.NewRequest(method, target, body)
	request.Header.Set("Authorization", "Bearer "+operatorToken)
	return request
}

func fixedSampleTime() time.Time {
	return time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
}

func orderPayload(total int64) []byte {
	payload := orderevents.OrderCreatedV1{
		OrderID:      "ord-1",
		UserID:       "usr-1",
		RestaurantID: "rst-pizza-1",
		Status:       orderevents.PendingStatus,
		TotalCents:   total,
		Items: []orderevents.OrderCreatedItemV1{{
			MenuItemID:      "item-margherita",
			Name:            "Margherita",
			UnitPriceCents:  total,
			Quantity:        1,
			ModifierItemIDs: []string{},
			LineTotalCents:  total,
		}},
	}
	body, _ := json.Marshal(payload)
	return body
}
