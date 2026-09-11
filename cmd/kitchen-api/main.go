package main

import (
	"context"
	"log"

	catalogv1 "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/gen/go/catalog/v1"
	orderv1 "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/gen/go/order/v1"
	cataloggrpcclient "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/grpcclient"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/kitchen/httpapi"
	ordergrpcclient "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/grpcclient"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/runtime"
)

func main() {
	config, err := runtime.ConfigFromEnv("kitchen-api")
	if err != nil {
		log.Fatal(err)
	}

	catalogConn, err := runtime.NewGRPCClientConn(config.CatalogGRPCTarget)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = catalogConn.Close() }()

	orderConn, err := runtime.NewGRPCClientConn(config.OrderGRPCTarget)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = orderConn.Close() }()

	handler := httpapi.NewHandler(httpapi.Dependencies{
		Catalog: cataloggrpcclient.New(catalogv1.NewCatalogServiceClient(catalogConn)),
		Orders:  ordergrpcclient.New(orderv1.NewOrderServiceClient(orderConn)),
	})

	log.Printf("starting %s http=%s catalog=%s order=%s", config.ServiceName, config.HTTPAddr, config.CatalogGRPCTarget, config.OrderGRPCTarget)
	if err := runtime.RunHTTPServer(context.Background(), config, handler); err != nil {
		log.Fatal(err)
	}
}
