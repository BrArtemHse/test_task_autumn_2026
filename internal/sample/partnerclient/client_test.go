package partnerclient

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/contracts/partnercallbacks"
)

func TestClientSendsAuthenticatedStatusCallback(t *testing.T) {
	var gotAuthorization string
	var gotRequestID string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		gotAuthorization = request.Header.Get("Authorization")
		gotRequestID = request.Header.Get("X-Request-ID")
		writer.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(server.Close)
	client, err := New(Config{
		URL:     server.URL + "/callbacks/order-status",
		Token:   "integration-token",
		Timeout: time.Second,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := client.Send(context.Background(), callbackPayload()); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if gotAuthorization != "Bearer integration-token" {
		t.Fatalf("authorization = %q", gotAuthorization)
	}
	if gotRequestID != "callback-1" {
		t.Fatalf("request id = %q, want callback-1", gotRequestID)
	}
}

func TestClientClassifiesPermanentAndRetryableResponses(t *testing.T) {
	for _, test := range []struct {
		name      string
		status    int
		permanent bool
	}{
		{name: "redirect", status: http.StatusFound, permanent: true},
		{name: "conflict", status: http.StatusConflict, permanent: true},
		{name: "server error", status: http.StatusServiceUnavailable, permanent: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(test.status)
			}))
			t.Cleanup(server.Close)
			client, err := New(Config{URL: server.URL, Token: "integration-token", Timeout: time.Second})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}

			err = client.Send(context.Background(), callbackPayload())
			if err == nil {
				t.Fatal("Send() error = nil")
			}
			if IsPermanent(err) != test.permanent {
				t.Fatalf("IsPermanent(%v) = %t, want %t", err, IsPermanent(err), test.permanent)
			}
		})
	}
}

func TestClientDoesNotFollowRedirects(t *testing.T) {
	followed := false
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		followed = true
		writer.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(target.Close)
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL, http.StatusFound)
	}))
	t.Cleanup(redirect.Close)
	client, err := New(Config{URL: redirect.URL, Token: "integration-token", Timeout: time.Second})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	err = client.Send(context.Background(), callbackPayload())
	if !IsPermanent(err) {
		t.Fatalf("Send() error = %v, want permanent redirect error", err)
	}
	if followed {
		t.Fatal("redirect target was called")
	}
}

func TestClientRejectsInvalidConfigAndPayload(t *testing.T) {
	_, err := New(Config{URL: "", Token: "integration-token", Timeout: time.Second})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New() error = %v, want %v", err, ErrInvalidConfig)
	}
	client, err := New(Config{URL: "http://partner-service/callback", Token: "integration-token", Timeout: time.Second})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	err = client.Send(context.Background(), partnercallbacks.OrderStatusV1{})
	if !errors.Is(err, ErrInvalidCallback) {
		t.Fatalf("Send() error = %v, want %v", err, ErrInvalidCallback)
	}
}

func callbackPayload() partnercallbacks.OrderStatusV1 {
	return partnercallbacks.OrderStatusV1{
		CallbackID:      "callback-1",
		OrderID:         "order-1",
		RestaurantID:    "restaurant-1",
		ExternalStoreID: "store-1",
		Status:          "accepted",
		OccurredAt:      time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC),
	}
}
