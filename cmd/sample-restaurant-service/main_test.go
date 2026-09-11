package main

import (
	"errors"
	"testing"
	"time"

	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/runtime"
	samplehttp "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/sample/httpapi"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/sample/partnerclient"
)

func TestNewSampleHandlerRejectsMissingCallbackURL(t *testing.T) {
	_, err := newSampleHandler(runtime.Config{
		PartnerCallbackToken:   "integration-token",
		PartnerCallbackTimeout: time.Second,
	})
	if !errors.Is(err, partnerclient.ErrInvalidConfig) {
		t.Fatalf("newSampleHandler() error = %v, want %v", err, partnerclient.ErrInvalidConfig)
	}
}

func TestNewSampleHandlerRejectsMissingOperatorToken(t *testing.T) {
	_, err := newSampleHandler(runtime.Config{
		PartnerCallbackURL:     "http://partner-service:8083/callbacks/order-status",
		PartnerCallbackToken:   "integration-token",
		PartnerCallbackTimeout: time.Second,
	})
	if !errors.Is(err, samplehttp.ErrInvalidConfig) {
		t.Fatalf("newSampleHandler() error = %v, want %v", err, samplehttp.ErrInvalidConfig)
	}
}
