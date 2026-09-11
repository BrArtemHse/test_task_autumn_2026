package main

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	partnerapp "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/application"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/callback"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/dispatch"
	partnerhttpapi "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/httpapi"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/kafkahandler"
	partnerpostgres "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/postgres"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/restaurantclient"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/id"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/kafkaconsume"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/kafkapub"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/migrations"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/outbox"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/postgresdb"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/runtime"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	config, err := runtime.ConfigFromEnv("partner-service")
	if err != nil {
		log.Fatal(err)
	}

	db, err := postgresdb.Open(ctx, config.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	store, err := newPartnerStore(ctx, db, "migrations/partner")
	if err != nil {
		log.Fatal(err)
	}
	orderEventService := partnerapp.NewService(partnerapp.ServiceConfig{
		Store: store,
		NewID: id.NewString,
	})
	consumer, err := newOrderConsumer(config, orderEventService)
	if err != nil {
		log.Fatal(err)
	}
	callbackService := callback.NewService(callback.ServiceConfig{
		Store: partnerpostgres.NewCallbackStore(db),
		NewID: id.NewString,
		Now:   func() time.Time { return time.Now().UTC() },
	})
	partnerRelay, relayCloser, err := newPartnerRelay(db, config, config.ServiceName+"-"+id.NewString())
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = relayCloser.Close() }()
	defer func() { _ = consumer.Close() }()
	dispatcher, err := newSubmissionDispatcher(db, config, config.ServiceName+"-"+id.NewString())
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("partner order consumer stopped: %v", err)
		}
	}()
	go func() {
		if err := dispatcher.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("partner submission dispatcher stopped: %v", err)
		}
	}()
	go func() {
		if err := partnerRelay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("partner outbox relay stopped: %v", err)
		}
	}()

	log.Printf(
		"starting %s http=%s topic=%s group=%s",
		config.ServiceName,
		config.HTTPAddr,
		config.OrderEventsTopic,
		config.OrderEventsConsumerGroup,
	)
	if err := runtime.RunHTTPServer(ctx, config, newPartnerHTTPHandler(config.ServiceName, callbackService)); err != nil {
		log.Fatal(err)
	}
}

func newPartnerHTTPHandler(serviceName string, callbackService partnerhttpapi.StatusCallbackService) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/healthz", runtime.NewHealthHandler(serviceName))
	mux.Handle("/callbacks/order-status", partnerhttpapi.NewHandler(callbackService))
	return mux
}

func newPartnerRelay(db *sql.DB, config runtime.Config, relayID string) (*outbox.Relay, io.Closer, error) {
	store, err := partnerpostgres.NewOutboxStore(db, partnerpostgres.OutboxConfig{
		RelayID:    relayID,
		Topic:      config.PartnerEventsTopic,
		LockTTL:    config.OutboxRelayLockTTL,
		RetryDelay: config.OutboxRelayRetryDelay,
	})
	if err != nil {
		return nil, nil, err
	}
	publisher, err := kafkapub.NewPublisher(config.KafkaBrokers)
	if err != nil {
		return nil, nil, err
	}
	relay, err := outbox.NewRelay(outbox.Config{
		BatchSize:    config.OutboxRelayBatchSize,
		PollInterval: config.OutboxRelayPollInterval,
		Store:        store,
		Publisher:    publisher,
		ErrorHandler: func(err error) {
			log.Printf("partner outbox relay error: %v", err)
		},
	})
	if err != nil {
		_ = publisher.Close()
		return nil, nil, err
	}
	return relay, publisher, nil
}

func newSubmissionDispatcher(db *sql.DB, config runtime.Config, dispatcherID string) (*dispatch.Dispatcher, error) {
	store, err := partnerpostgres.NewSubmissionStore(db, partnerpostgres.SubmissionConfig{
		DispatcherID: dispatcherID,
		LockTTL:      config.PartnerDispatchLockTTL,
		RetryDelay:   config.PartnerDispatchRetryDelay,
	})
	if err != nil {
		return nil, err
	}
	client, err := restaurantclient.New(restaurantclient.Config{Timeout: config.PartnerHTTPTimeout})
	if err != nil {
		return nil, err
	}
	return dispatch.New(dispatch.Config{
		BatchSize:    config.PartnerDispatchBatchSize,
		PollInterval: config.PartnerDispatchPollInterval,
		MaxAttempts:  config.PartnerDispatchMaxAttempts,
		Store:        store,
		Sender:       client,
		ErrorHandler: func(err error) {
			log.Printf("partner submission dispatcher error: %v", err)
		},
	})
}

func newPartnerStore(ctx context.Context, db *sql.DB, migrationDir string) (partnerapp.Store, error) {
	if err := migrations.Run(ctx, db, migrations.Config{
		Service:   "partner-service",
		Directory: migrationDir,
	}); err != nil {
		return nil, err
	}
	return partnerpostgres.NewStore(db), nil
}

func newOrderConsumer(config runtime.Config, service *partnerapp.Service) (*kafkaconsume.Consumer, error) {
	handler, err := kafkahandler.NewOrderCreatedHandler(service)
	if err != nil {
		return nil, err
	}
	return kafkaconsume.New(kafkaconsume.Config{
		Brokers:    config.KafkaBrokers,
		Topic:      config.OrderEventsTopic,
		GroupID:    config.OrderEventsConsumerGroup,
		RetryDelay: config.KafkaConsumerRetryDelay,
		Handler:    handler,
		ErrorHandler: func(err error) {
			log.Printf("partner order consumer error: %v", err)
		},
	})
}
