package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func TestConfigFromLookupBuildsServiceConfig(t *testing.T) {
	config, err := ConfigFromLookup("order-service", mapLookup(map[string]string{
		"HTTP_ADDR":                        ":18082",
		"GRPC_ADDR":                        ":19082",
		"CATALOG_GRPC_TARGET":              "dns:///catalog-service:9081",
		"ORDER_GRPC_TARGET":                "dns:///order-service:9082",
		"DATABASE_URL":                     "postgres://orders:orders@postgres:5432/orders",
		"KAFKA_BROKERS":                    "kafka:9092,localhost:19092",
		"ORDER_EVENTS_TOPIC":               "orders.events.custom",
		"ORDER_EVENTS_CONSUMER_GROUP":      "partner-orders-custom",
		"PARTNER_EVENTS_TOPIC":             "partner.events.custom",
		"PARTNER_EVENTS_CONSUMER_GROUP":    "order-partner-custom",
		"KAFKA_CONSUMER_RETRY_DELAY":       "750ms",
		"PARTNER_DISPATCH_BATCH_SIZE":      "12",
		"PARTNER_DISPATCH_POLL_INTERVAL":   "300ms",
		"PARTNER_DISPATCH_LOCK_TTL":        "40s",
		"PARTNER_DISPATCH_RETRY_DELAY":     "8s",
		"PARTNER_DISPATCH_MAX_ATTEMPTS":    "7",
		"PARTNER_HTTP_TIMEOUT":             "4s",
		"PARTNER_CALLBACK_URL":             "http://partner-service:8083/callbacks/order-status",
		"PARTNER_CALLBACK_TOKEN":           "integration-token",
		"PARTNER_CALLBACK_TIMEOUT":         "2500ms",
		"SAMPLE_RESTAURANT_OPERATOR_TOKEN": "sample-operator-token",
		"OUTBOX_RELAY_BATCH_SIZE":          "25",
		"OUTBOX_RELAY_POLL_INTERVAL":       "250ms",
		"OUTBOX_RELAY_LOCK_TTL":            "45s",
		"OUTBOX_RELAY_RETRY_DELAY":         "9s",
		"SHUTDOWN_TIMEOUT":                 "7s",
	}))
	if err != nil {
		t.Fatalf("ConfigFromLookup() error = %v", err)
	}

	if config.ServiceName != "order-service" {
		t.Fatalf("service name = %q, want order-service", config.ServiceName)
	}
	if config.HTTPAddr != ":18082" {
		t.Fatalf("http addr = %q, want :18082", config.HTTPAddr)
	}
	if config.GRPCAddr != ":19082" {
		t.Fatalf("grpc addr = %q, want :19082", config.GRPCAddr)
	}
	if config.CatalogGRPCTarget != "dns:///catalog-service:9081" {
		t.Fatalf("catalog grpc target = %q, want dns:///catalog-service:9081", config.CatalogGRPCTarget)
	}
	if config.OrderGRPCTarget != "dns:///order-service:9082" {
		t.Fatalf("order grpc target = %q, want dns:///order-service:9082", config.OrderGRPCTarget)
	}
	if config.DatabaseURL != "postgres://orders:orders@postgres:5432/orders" {
		t.Fatalf("database url = %q", config.DatabaseURL)
	}
	if !reflect.DeepEqual(config.KafkaBrokers, []string{"kafka:9092", "localhost:19092"}) {
		t.Fatalf("kafka brokers = %#v", config.KafkaBrokers)
	}
	if config.OrderEventsTopic != "orders.events.custom" {
		t.Fatalf("order events topic = %q, want orders.events.custom", config.OrderEventsTopic)
	}
	if config.OrderEventsConsumerGroup != "partner-orders-custom" {
		t.Fatalf("order events consumer group = %q, want partner-orders-custom", config.OrderEventsConsumerGroup)
	}
	if config.PartnerEventsTopic != "partner.events.custom" {
		t.Fatalf("partner events topic = %q, want partner.events.custom", config.PartnerEventsTopic)
	}
	if config.PartnerEventsConsumerGroup != "order-partner-custom" {
		t.Fatalf("partner events consumer group = %q, want order-partner-custom", config.PartnerEventsConsumerGroup)
	}
	if config.KafkaConsumerRetryDelay != 750*time.Millisecond {
		t.Fatalf("kafka consumer retry delay = %s, want 750ms", config.KafkaConsumerRetryDelay)
	}
	if config.PartnerDispatchBatchSize != 12 ||
		config.PartnerDispatchPollInterval != 300*time.Millisecond ||
		config.PartnerDispatchLockTTL != 40*time.Second ||
		config.PartnerDispatchRetryDelay != 8*time.Second ||
		config.PartnerDispatchMaxAttempts != 7 ||
		config.PartnerHTTPTimeout != 4*time.Second {
		t.Fatalf("partner dispatch config = %#v", config)
	}
	if config.PartnerCallbackURL != "http://partner-service:8083/callbacks/order-status" ||
		config.PartnerCallbackToken != "integration-token" ||
		config.PartnerCallbackTimeout != 2500*time.Millisecond {
		t.Fatalf("partner callback config = %#v", config)
	}
	if config.SampleRestaurantOperatorToken != "sample-operator-token" {
		t.Fatalf("sample restaurant operator token = %q", config.SampleRestaurantOperatorToken)
	}
	if config.OutboxRelayBatchSize != 25 {
		t.Fatalf("outbox relay batch size = %d, want 25", config.OutboxRelayBatchSize)
	}
	if config.OutboxRelayPollInterval != 250*time.Millisecond {
		t.Fatalf("outbox relay poll interval = %s, want 250ms", config.OutboxRelayPollInterval)
	}
	if config.OutboxRelayLockTTL != 45*time.Second {
		t.Fatalf("outbox relay lock ttl = %s, want 45s", config.OutboxRelayLockTTL)
	}
	if config.OutboxRelayRetryDelay != 9*time.Second {
		t.Fatalf("outbox relay retry delay = %s, want 9s", config.OutboxRelayRetryDelay)
	}
	if config.ShutdownTimeout != 7*time.Second {
		t.Fatalf("shutdown timeout = %s, want 7s", config.ShutdownTimeout)
	}
}

func TestConfigFromLookupUsesSafeDefaults(t *testing.T) {
	config, err := ConfigFromLookup("catalog-service", mapLookup(nil))
	if err != nil {
		t.Fatalf("ConfigFromLookup() error = %v", err)
	}

	if config.HTTPAddr != ":8080" {
		t.Fatalf("http addr = %q, want :8080", config.HTTPAddr)
	}
	if config.GRPCAddr != ":9090" {
		t.Fatalf("grpc addr = %q, want :9090", config.GRPCAddr)
	}
	if config.ShutdownTimeout != 10*time.Second {
		t.Fatalf("shutdown timeout = %s, want 10s", config.ShutdownTimeout)
	}
	if config.OrderEventsTopic != "orders.events.v1" {
		t.Fatalf("order events topic = %q, want orders.events.v1", config.OrderEventsTopic)
	}
	if config.OrderEventsConsumerGroup != "partner-service-orders-v1" {
		t.Fatalf("order events consumer group = %q, want partner-service-orders-v1", config.OrderEventsConsumerGroup)
	}
	if config.PartnerEventsTopic != "partner.events.v1" {
		t.Fatalf("partner events topic = %q, want partner.events.v1", config.PartnerEventsTopic)
	}
	if config.PartnerEventsConsumerGroup != "order-service-partner-events-v1" {
		t.Fatalf("partner events consumer group = %q, want order-service-partner-events-v1", config.PartnerEventsConsumerGroup)
	}
	if config.KafkaConsumerRetryDelay != time.Second {
		t.Fatalf("kafka consumer retry delay = %s, want 1s", config.KafkaConsumerRetryDelay)
	}
	if config.PartnerDispatchBatchSize != 20 ||
		config.PartnerDispatchPollInterval != time.Second ||
		config.PartnerDispatchLockTTL != 30*time.Second ||
		config.PartnerDispatchRetryDelay != 5*time.Second ||
		config.PartnerDispatchMaxAttempts != 5 ||
		config.PartnerHTTPTimeout != 3*time.Second {
		t.Fatalf("default partner dispatch config = %#v", config)
	}
	if config.PartnerCallbackTimeout != 3*time.Second {
		t.Fatalf("partner callback timeout = %s, want 3s", config.PartnerCallbackTimeout)
	}
	if config.OutboxRelayBatchSize != 50 {
		t.Fatalf("outbox relay batch size = %d, want 50", config.OutboxRelayBatchSize)
	}
	if config.OutboxRelayPollInterval != time.Second {
		t.Fatalf("outbox relay poll interval = %s, want 1s", config.OutboxRelayPollInterval)
	}
	if config.OutboxRelayLockTTL != 30*time.Second {
		t.Fatalf("outbox relay lock ttl = %s, want 30s", config.OutboxRelayLockTTL)
	}
	if config.OutboxRelayRetryDelay != 5*time.Second {
		t.Fatalf("outbox relay retry delay = %s, want 5s", config.OutboxRelayRetryDelay)
	}
}

func TestConfigFromLookupRejectsInvalidServiceName(t *testing.T) {
	_, err := ConfigFromLookup("", mapLookup(nil))
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("ConfigFromLookup() error = %v, want %v", err, ErrInvalidConfig)
	}
}

func TestConfigFromLookupRejectsInvalidShutdownTimeout(t *testing.T) {
	_, err := ConfigFromLookup("order-service", mapLookup(map[string]string{
		"SHUTDOWN_TIMEOUT": "forever",
	}))
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("ConfigFromLookup() error = %v, want %v", err, ErrInvalidConfig)
	}
}

func TestConfigFromLookupRejectsInvalidOutboxRelayBatchSize(t *testing.T) {
	_, err := ConfigFromLookup("order-service", mapLookup(map[string]string{
		"OUTBOX_RELAY_BATCH_SIZE": "0",
	}))
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("ConfigFromLookup() error = %v, want %v", err, ErrInvalidConfig)
	}
}

func TestConfigFromLookupRejectsInvalidOutboxRelayDuration(t *testing.T) {
	_, err := ConfigFromLookup("order-service", mapLookup(map[string]string{
		"OUTBOX_RELAY_LOCK_TTL": "later",
	}))
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("ConfigFromLookup() error = %v, want %v", err, ErrInvalidConfig)
	}
}

func TestConfigFromLookupRejectsInvalidKafkaConsumerRetryDelay(t *testing.T) {
	_, err := ConfigFromLookup("partner-service", mapLookup(map[string]string{
		"KAFKA_CONSUMER_RETRY_DELAY": "-1s",
	}))
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("ConfigFromLookup() error = %v, want %v", err, ErrInvalidConfig)
	}
}

func TestConfigFromLookupRejectsInvalidPartnerDispatchMaxAttempts(t *testing.T) {
	_, err := ConfigFromLookup("partner-service", mapLookup(map[string]string{
		"PARTNER_DISPATCH_MAX_ATTEMPTS": "0",
	}))
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("ConfigFromLookup() error = %v, want %v", err, ErrInvalidConfig)
	}
}

func TestHealthHandlerReturnsServiceStatus(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	recorder := httptest.NewRecorder()

	NewHealthHandler("partner-service").ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", recorder.Code, http.StatusOK)
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("content type = %q, want application/json", got)
	}

	var body struct {
		Service string `json:"service"`
		Status  string `json:"status"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("response JSON error = %v", err)
	}
	if body.Service != "partner-service" || body.Status != "ok" {
		t.Fatalf("body = %#v", body)
	}
}

func TestRunHTTPAndGRPCServersRejectsMissingRegisterFunc(t *testing.T) {
	config := Config{
		ServiceName:     "catalog-service",
		HTTPAddr:        "127.0.0.1:0",
		GRPCAddr:        "127.0.0.1:0",
		ShutdownTimeout: time.Second,
	}

	err := RunHTTPAndGRPCServers(context.Background(), config, nil, nil)

	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("RunHTTPAndGRPCServers() error = %v, want %v", err, ErrInvalidConfig)
	}
}

func mapLookup(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}
