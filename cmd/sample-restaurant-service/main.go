package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/id"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/runtime"
	samplehttp "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/sample/httpapi"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/sample/partnerclient"
)

func main() {
	config, err := runtime.ConfigFromEnv("sample-restaurant-service")
	if err != nil {
		log.Fatal(err)
	}
	handler, err := newSampleHandler(config)
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.Handle("/healthz", runtime.NewHealthHandler(config.ServiceName))
	mux.Handle("/", handler)

	log.Printf("starting %s on %s", config.ServiceName, config.HTTPAddr)
	if err := runtime.RunHTTPServer(context.Background(), config, mux); err != nil {
		log.Fatal(err)
	}
}

func newSampleHandler(config runtime.Config) (http.Handler, error) {
	client, err := partnerclient.New(partnerclient.Config{
		URL:     config.PartnerCallbackURL,
		Token:   config.PartnerCallbackToken,
		Timeout: config.PartnerCallbackTimeout,
	})
	if err != nil {
		return nil, err
	}
	operatorToken := strings.TrimSpace(config.SampleRestaurantOperatorToken)
	if operatorToken == "" || strings.ContainsAny(operatorToken, " \t\r\n") {
		return nil, fmt.Errorf("%w: operator token is required", samplehttp.ErrInvalidConfig)
	}
	return samplehttp.NewHandlerWithConfig(samplehttp.Config{
		Sender:        client,
		NewID:         id.NewString,
		Now:           func() time.Time { return time.Now().UTC() },
		OperatorToken: operatorToken,
	}), nil
}
