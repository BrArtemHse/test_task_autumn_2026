package main

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log"
	"os/signal"
	"syscall"
	"time"

	catalogv1 "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/gen/go/catalog/v1"
	orderv1 "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/gen/go/order/v1"
	cataloggrpcclient "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/grpcclient"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/application"
	ordergrpcapi "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/grpcapi"
	orderkafkahandler "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/kafkahandler"
	orderpostgres "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/postgres"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/id"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/kafkaconsume"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/kafkapub"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/migrations"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/outbox"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/postgresdb"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/runtime"
	"google.golang.org/grpc"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	config, err := runtime.ConfigFromEnv("order-service")
	if err != nil {
		log.Fatal(err)
	}

	db, err := postgresdb.Open(ctx, config.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	orderStore, err := newOrderStore(ctx, db, "migrations/order")
	if err != nil {
		log.Fatal(err)
	}

	orderRelay, relayCloser, err := newOrderRelay(db, config, config.ServiceName+"-"+id.NewString())
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = relayCloser.Close() }()
	go func() {
		if err := orderRelay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("order outbox relay stopped: %v", err)
		}
	}()

	catalogConn, err := runtime.NewGRPCClientConn(config.CatalogGRPCTarget)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = catalogConn.Close() }()

	orderService := application.NewService(application.ServiceConfig{
		Catalog: cataloggrpcclient.New(catalogv1.NewCatalogServiceClient(catalogConn)),
		Store:   orderStore,
		NewID:   id.NewString,
		Now:     func() time.Time { return time.Now().UTC() },
	})
	partnerConsumer, err := newPartnerEventConsumer(config, orderService)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = partnerConsumer.Close() }()
	go func() {
		if err := partnerConsumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("partner event consumer stopped: %v", err)
		}
	}()

	log.Printf(
		"starting %s http=%s grpc=%s catalog=%s order_events_topic=%s partner_events_topic=%s",
		config.ServiceName,
		config.HTTPAddr,
		config.GRPCAddr,
		config.CatalogGRPCTarget,
		config.OrderEventsTopic,
		config.PartnerEventsTopic,
	)
	if err := runtime.RunHTTPAndGRPCServers(ctx, config, nil, func(server *grpc.Server) {
		orderv1.RegisterOrderServiceServer(server, ordergrpcapi.NewServer(orderService))
	}); err != nil {
		log.Fatal(err)
	}
}

func newPartnerEventConsumer(config runtime.Config, service orderkafkahandler.PartnerEventService) (*kafkaconsume.Consumer, error) {
	handler, err := orderkafkahandler.NewPartnerStatusHandler(service)
	if err != nil {
		return nil, err
	}
	return kafkaconsume.New(kafkaconsume.Config{
		Brokers:    config.KafkaBrokers,
		Topic:      config.PartnerEventsTopic,
		GroupID:    config.PartnerEventsConsumerGroup,
		RetryDelay: config.KafkaConsumerRetryDelay,
		Handler:    handler,
		ErrorHandler: func(err error) {
			log.Printf("partner event consumer error: %v", err)
		},
	})
}

func newOrderStore(ctx context.Context, db *sql.DB, migrationDir string) (application.OrderStore, error) {
	if err := migrations.Run(ctx, db, migrations.Config{
		Service:   "order-service",
		Directory: migrationDir,
	}); err != nil {
		return nil, err
	}

	return orderpostgres.NewStore(db), nil
}

func newOrderRelay(db *sql.DB, config runtime.Config, relayID string) (*outbox.Relay, io.Closer, error) {
	outboxStore, err := orderpostgres.NewOutboxStore(db, orderpostgres.OutboxConfig{
		RelayID:    relayID,
		Topic:      config.OrderEventsTopic,
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
		Store:        outboxStore,
		Publisher:    publisher,
		ErrorHandler: func(err error) {
			log.Printf("order outbox relay error: %v", err)
		},
	})
	if err != nil {
		_ = publisher.Close()
		return nil, nil, err
	}

	return relay, publisher, nil
}
