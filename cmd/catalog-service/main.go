package main

import (
	"context"
	"database/sql"
	"log"
	"os/signal"
	"syscall"

	catalogv1 "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/gen/go/catalog/v1"
	cataloggrpcapi "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/grpcapi"
	catalogpostgres "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/postgres"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/migrations"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/postgresdb"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/runtime"
	"google.golang.org/grpc"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	config, err := runtime.ConfigFromEnv("catalog-service")
	if err != nil {
		log.Fatal(err)
	}

	db, err := postgresdb.Open(ctx, config.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	store, err := newCatalogStore(ctx, db, "migrations/catalog")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("starting %s http=%s grpc=%s", config.ServiceName, config.HTTPAddr, config.GRPCAddr)
	if err := runtime.RunHTTPAndGRPCServers(ctx, config, nil, func(server *grpc.Server) {
		catalogv1.RegisterCatalogServiceServer(server, cataloggrpcapi.NewServer(store))
	}); err != nil {
		log.Fatal(err)
	}
}

func newCatalogStore(ctx context.Context, db *sql.DB, migrationDir string) (cataloggrpcapi.Store, error) {
	if err := migrations.Run(ctx, db, migrations.Config{
		Service:   "catalog-service",
		Directory: migrationDir,
	}); err != nil {
		return nil, err
	}
	return catalogpostgres.NewStore(db), nil
}
