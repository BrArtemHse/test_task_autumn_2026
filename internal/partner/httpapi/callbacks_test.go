package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/callback"
)

func TestHandlerAcceptsFirstStatusCallback(t *testing.T) {
	service := &callbackServiceStub{}
	handler := NewHandler(service)
	request := callbackRequest(validCallbackBody())
	request.Header.Set("Authorization", "Bearer integration-token")
	request.Header.Set("X-Request-ID", "request-1")
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusAccepted, recorder.Body.String())
	}
	if service.input.BearerToken != "integration-token" || service.input.RequestID != "request-1" {
		t.Fatalf("auth/request input = %#v", service.input)
	}
	if service.input.CallbackID != "callback-1" ||
		service.input.OrderID != "order-1" ||
		service.input.RestaurantID != "restaurant-1" ||
		service.input.ExternalStoreID != "store-1" ||
		service.input.Status != "accepted" {
		t.Fatalf("callback input = %#v", service.input)
	}
}

func TestHandlerReturnsOKForDuplicateCallback(t *testing.T) {
	service := &callbackServiceStub{result: callback.ReceiveResult{Duplicate: true}}
	handler := NewHandler(service)
	request := callbackRequest(validCallbackBody())
	request.Header.Set("Authorization", "Bearer integration-token")
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if !strings.Contains(recorder.Body.String(), `"duplicate":true`) {
		t.Fatalf("body = %s", recorder.Body.String())
	}
}

func TestHandlerRequiresStrictBearerAuthorization(t *testing.T) {
	for _, authorization := range []string{"", "integration-token", "bearer integration-token", "Bearer "} {
		t.Run(authorization, func(t *testing.T) {
			service := &callbackServiceStub{}
			handler := NewHandler(service)
			request := callbackRequest(validCallbackBody())
			request.Header.Set("Authorization", authorization)
			recorder := httptest.NewRecorder()

			handler.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
			}
			if service.called {
				t.Fatal("service called without valid bearer authorization")
			}
		})
	}
}

func TestHandlerMapsCallbackErrors(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{name: "invalid", err: callback.ErrInvalidStatusCallback, wantStatus: http.StatusBadRequest},
		{name: "unauthorized", err: callback.ErrUnauthorizedCallback, wantStatus: http.StatusUnauthorized},
		{name: "conflict", err: callback.ErrCallbackConflict, wantStatus: http.StatusConflict},
		{name: "storage", err: errors.New("database unavailable"), wantStatus: http.StatusInternalServerError},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &callbackServiceStub{err: test.err}
			handler := NewHandler(service)
			request := callbackRequest(validCallbackBody())
			request.Header.Set("Authorization", "Bearer integration-token")
			recorder := httptest.NewRecorder()

			handler.ServeHTTP(recorder, request)

			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, test.wantStatus)
			}
		})
	}
}

func TestHandlerRejectsInvalidOrOversizedJSON(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "invalid", body: `{"callback_id":`},
		{name: "multiple objects", body: validCallbackBody() + validCallbackBody()},
		{name: "oversized", body: `{"padding":"` + strings.Repeat("x", maxCallbackBodyBytes) + `"}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := &callbackServiceStub{}
			handler := NewHandler(service)
			request := callbackRequest(test.body)
			request.Header.Set("Authorization", "Bearer integration-token")
			recorder := httptest.NewRecorder()

			handler.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
			}
			if service.called {
				t.Fatal("service called for invalid JSON")
			}
		})
	}
}

type callbackServiceStub struct {
	called bool
	input  callback.ReceiveStatusCallbackInput
	result callback.ReceiveResult
	err    error
}

func (s *callbackServiceStub) ReceiveStatusCallback(_ context.Context, input callback.ReceiveStatusCallbackInput) (callback.ReceiveResult, error) {
	s.called = true
	s.input = input
	return s.result, s.err
}

func callbackRequest(body string) *http.Request {
	return httptest.NewRequest(http.MethodPost, "/callbacks/order-status", strings.NewReader(body))
}

func validCallbackBody() string {
	return `{"callback_id":"callback-1","order_id":"order-1","restaurant_id":"restaurant-1","external_store_id":"store-1","status":"accepted","occurred_at":"2026-09-07T12:00:00Z"}`
}
