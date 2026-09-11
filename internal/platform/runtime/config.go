package runtime

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

var ErrInvalidConfig = errors.New("invalid config")

const (
	defaultOrderEventsTopic           = "orders.events.v1"
	defaultOrderEventsConsumerGroup   = "partner-service-orders-v1"
	defaultPartnerEventsTopic         = "partner.events.v1"
	defaultPartnerEventsConsumerGroup = "order-service-partner-events-v1"
	defaultKafkaConsumerRetryDelay    = time.Second
	defaultPartnerDispatchBatchSize   = 20
	defaultPartnerDispatchPoll        = time.Second
	defaultPartnerDispatchLockTTL     = 30 * time.Second
	defaultPartnerDispatchRetry       = 5 * time.Second
	defaultPartnerDispatchAttempts    = 5
	defaultPartnerHTTPTimeout         = 3 * time.Second
	defaultPartnerCallbackTimeout     = 3 * time.Second
	defaultOutboxRelayBatchSize       = 50
	defaultOutboxRelayPollInterval    = time.Second
	defaultOutboxRelayLockTTL         = 30 * time.Second
	defaultOutboxRelayRetryDelay      = 5 * time.Second
)

type Config struct {
	ServiceName                   string
	HTTPAddr                      string
	GRPCAddr                      string
	CatalogGRPCTarget             string
	OrderGRPCTarget               string
	DatabaseURL                   string
	KafkaBrokers                  []string
	OrderEventsTopic              string
	OrderEventsConsumerGroup      string
	PartnerEventsTopic            string
	PartnerEventsConsumerGroup    string
	KafkaConsumerRetryDelay       time.Duration
	PartnerDispatchBatchSize      int
	PartnerDispatchPollInterval   time.Duration
	PartnerDispatchLockTTL        time.Duration
	PartnerDispatchRetryDelay     time.Duration
	PartnerDispatchMaxAttempts    int
	PartnerHTTPTimeout            time.Duration
	PartnerCallbackURL            string
	PartnerCallbackToken          string
	PartnerCallbackTimeout        time.Duration
	SampleRestaurantOperatorToken string
	OutboxRelayBatchSize          int
	OutboxRelayPollInterval       time.Duration
	OutboxRelayLockTTL            time.Duration
	OutboxRelayRetryDelay         time.Duration
	ShutdownTimeout               time.Duration
}

func ConfigFromEnv(serviceName string) (Config, error) {
	return ConfigFromLookup(serviceName, os.LookupEnv)
}

func ConfigFromLookup(serviceName string, lookup func(string) (string, bool)) (Config, error) {
	if strings.TrimSpace(serviceName) == "" {
		return Config{}, fmt.Errorf("%w: service name is required", ErrInvalidConfig)
	}

	shutdownTimeout, err := lookupDuration(lookup, "SHUTDOWN_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, fmt.Errorf("%w: shutdown timeout: %v", ErrInvalidConfig, err)
	}
	outboxBatchSize, err := lookupPositiveInt(lookup, "OUTBOX_RELAY_BATCH_SIZE", defaultOutboxRelayBatchSize)
	if err != nil {
		return Config{}, fmt.Errorf("%w: outbox relay batch size: %v", ErrInvalidConfig, err)
	}
	outboxPollInterval, err := lookupDuration(lookup, "OUTBOX_RELAY_POLL_INTERVAL", defaultOutboxRelayPollInterval)
	if err != nil {
		return Config{}, fmt.Errorf("%w: outbox relay poll interval: %v", ErrInvalidConfig, err)
	}
	outboxLockTTL, err := lookupDuration(lookup, "OUTBOX_RELAY_LOCK_TTL", defaultOutboxRelayLockTTL)
	if err != nil {
		return Config{}, fmt.Errorf("%w: outbox relay lock ttl: %v", ErrInvalidConfig, err)
	}
	outboxRetryDelay, err := lookupDuration(lookup, "OUTBOX_RELAY_RETRY_DELAY", defaultOutboxRelayRetryDelay)
	if err != nil {
		return Config{}, fmt.Errorf("%w: outbox relay retry delay: %v", ErrInvalidConfig, err)
	}
	consumerRetryDelay, err := lookupDuration(lookup, "KAFKA_CONSUMER_RETRY_DELAY", defaultKafkaConsumerRetryDelay)
	if err != nil {
		return Config{}, fmt.Errorf("%w: kafka consumer retry delay: %v", ErrInvalidConfig, err)
	}
	partnerDispatchBatchSize, err := lookupPositiveInt(lookup, "PARTNER_DISPATCH_BATCH_SIZE", defaultPartnerDispatchBatchSize)
	if err != nil {
		return Config{}, fmt.Errorf("%w: partner dispatch batch size: %v", ErrInvalidConfig, err)
	}
	partnerDispatchPoll, err := lookupDuration(lookup, "PARTNER_DISPATCH_POLL_INTERVAL", defaultPartnerDispatchPoll)
	if err != nil {
		return Config{}, fmt.Errorf("%w: partner dispatch poll interval: %v", ErrInvalidConfig, err)
	}
	partnerDispatchLockTTL, err := lookupDuration(lookup, "PARTNER_DISPATCH_LOCK_TTL", defaultPartnerDispatchLockTTL)
	if err != nil {
		return Config{}, fmt.Errorf("%w: partner dispatch lock ttl: %v", ErrInvalidConfig, err)
	}
	partnerDispatchRetry, err := lookupDuration(lookup, "PARTNER_DISPATCH_RETRY_DELAY", defaultPartnerDispatchRetry)
	if err != nil {
		return Config{}, fmt.Errorf("%w: partner dispatch retry delay: %v", ErrInvalidConfig, err)
	}
	partnerDispatchAttempts, err := lookupPositiveInt(lookup, "PARTNER_DISPATCH_MAX_ATTEMPTS", defaultPartnerDispatchAttempts)
	if err != nil {
		return Config{}, fmt.Errorf("%w: partner dispatch max attempts: %v", ErrInvalidConfig, err)
	}
	partnerHTTPTimeout, err := lookupDuration(lookup, "PARTNER_HTTP_TIMEOUT", defaultPartnerHTTPTimeout)
	if err != nil {
		return Config{}, fmt.Errorf("%w: partner HTTP timeout: %v", ErrInvalidConfig, err)
	}
	partnerCallbackTimeout, err := lookupDuration(lookup, "PARTNER_CALLBACK_TIMEOUT", defaultPartnerCallbackTimeout)
	if err != nil {
		return Config{}, fmt.Errorf("%w: partner callback timeout: %v", ErrInvalidConfig, err)
	}

	return Config{
		ServiceName:                   serviceName,
		HTTPAddr:                      lookupDefault(lookup, "HTTP_ADDR", ":8080"),
		GRPCAddr:                      lookupDefault(lookup, "GRPC_ADDR", ":9090"),
		CatalogGRPCTarget:             lookupDefault(lookup, "CATALOG_GRPC_TARGET", ""),
		OrderGRPCTarget:               lookupDefault(lookup, "ORDER_GRPC_TARGET", ""),
		DatabaseURL:                   lookupDefault(lookup, "DATABASE_URL", ""),
		KafkaBrokers:                  splitCSV(lookupDefault(lookup, "KAFKA_BROKERS", "")),
		OrderEventsTopic:              lookupDefault(lookup, "ORDER_EVENTS_TOPIC", defaultOrderEventsTopic),
		OrderEventsConsumerGroup:      lookupDefault(lookup, "ORDER_EVENTS_CONSUMER_GROUP", defaultOrderEventsConsumerGroup),
		PartnerEventsTopic:            lookupDefault(lookup, "PARTNER_EVENTS_TOPIC", defaultPartnerEventsTopic),
		PartnerEventsConsumerGroup:    lookupDefault(lookup, "PARTNER_EVENTS_CONSUMER_GROUP", defaultPartnerEventsConsumerGroup),
		KafkaConsumerRetryDelay:       consumerRetryDelay,
		PartnerDispatchBatchSize:      partnerDispatchBatchSize,
		PartnerDispatchPollInterval:   partnerDispatchPoll,
		PartnerDispatchLockTTL:        partnerDispatchLockTTL,
		PartnerDispatchRetryDelay:     partnerDispatchRetry,
		PartnerDispatchMaxAttempts:    partnerDispatchAttempts,
		PartnerHTTPTimeout:            partnerHTTPTimeout,
		PartnerCallbackURL:            lookupDefault(lookup, "PARTNER_CALLBACK_URL", ""),
		PartnerCallbackToken:          lookupDefault(lookup, "PARTNER_CALLBACK_TOKEN", ""),
		PartnerCallbackTimeout:        partnerCallbackTimeout,
		SampleRestaurantOperatorToken: lookupDefault(lookup, "SAMPLE_RESTAURANT_OPERATOR_TOKEN", ""),
		OutboxRelayBatchSize:          outboxBatchSize,
		OutboxRelayPollInterval:       outboxPollInterval,
		OutboxRelayLockTTL:            outboxLockTTL,
		OutboxRelayRetryDelay:         outboxRetryDelay,
		ShutdownTimeout:               shutdownTimeout,
	}, nil
}

func lookupDefault(lookup func(string) (string, bool), key, fallback string) string {
	value, ok := lookup(key)
	if !ok || strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func splitCSV(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}

	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value != "" {
			values = append(values, value)
		}
	}

	return values
}

func lookupPositiveInt(lookup func(string) (string, bool), key string, fallback int) (int, error) {
	raw, ok := lookup(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, err
	}
	if value <= 0 {
		return 0, fmt.Errorf("must be positive")
	}
	return value, nil
}

func lookupDuration(lookup func(string) (string, bool), key string, fallback time.Duration) (time.Duration, error) {
	raw, ok := lookup(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		return 0, err
	}
	if value <= 0 {
		return 0, fmt.Errorf("must be positive")
	}
	return value, nil
}
