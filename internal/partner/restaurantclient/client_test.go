package restaurantclient_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/dispatch"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/restaurantclient"
)

func TestClientSendPostsIdempotentOrderSnapshot(t *testing.T) {
	var receivedBody string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}
		receivedBody = string(body)
		if request.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", request.Method)
		}
		if request.Header.Get("Idempotency-Key") != "ord-1" {
			t.Fatalf("idempotency key = %q", request.Header.Get("Idempotency-Key"))
		}
		if request.Header.Get("X-External-Store-ID") != "store-pizza-1" {
			t.Fatalf("external store id = %q", request.Header.Get("X-External-Store-ID"))
		}
		if request.Header.Get("X-Submission-ID") != "sub-1" {
			t.Fatalf("submission id = %q", request.Header.Get("X-Submission-ID"))
		}
		writer.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	client, err := restaurantclient.New(restaurantclient.Config{Timeout: time.Second})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	payload := []byte(`{"order_id":"ord-1"}`)
	err = client.Send(context.Background(), dispatch.Submission{
		ID:              "sub-1",
		OrderID:         "ord-1",
		ExternalStoreID: "store-pizza-1",
		DestinationURL:  server.URL,
		Payload:         payload,
		Attempt:         1,
	})
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if receivedBody != string(payload) {
		t.Fatalf("body = %q, want %q", receivedBody, payload)
	}
}

func TestClientSendClassifiesFourHundredsAsPermanent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "invalid order", http.StatusBadRequest)
	}))
	defer server.Close()
	client, err := restaurantclient.New(restaurantclient.Config{Timeout: time.Second})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	err = client.Send(context.Background(), validSubmission(server.URL))
	if !dispatch.IsPermanent(err) {
		t.Fatalf("Send() error = %v, want permanent", err)
	}
}

func TestClientSendClassifiesFiveHundredsAsRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client, err := restaurantclient.New(restaurantclient.Config{Timeout: time.Second})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	err = client.Send(context.Background(), validSubmission(server.URL))
	if err == nil {
		t.Fatal("Send() error = nil, want retryable error")
	}
	if dispatch.IsPermanent(err) {
		t.Fatalf("Send() error = %v, want retryable", err)
	}
}

func validSubmission(url string) dispatch.Submission {
	return dispatch.Submission{
		ID:              "sub-1",
		OrderID:         "ord-1",
		ExternalStoreID: "store-pizza-1",
		DestinationURL:  url,
		Payload:         []byte("{}"),
		Attempt:         1,
	}
}
