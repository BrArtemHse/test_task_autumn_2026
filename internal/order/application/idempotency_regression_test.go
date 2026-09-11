package application_test

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	catalogdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/catalog/domain"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/application"
	orderdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/domain"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/memory"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/id"
)

func TestCreateOrderReplayDoesNotDependOnCatalog(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "item unavailable", err: catalogdomain.ErrMenuItemUnavailable},
		{name: "restaurant closed", err: catalogdomain.ErrRestaurantClosed},
		{name: "catalog offline", err: application.ErrCatalogUnavailable},
		{name: "price changed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			catalog := &changingCatalog{price: 1200}
			store := memory.NewStore()
			service := application.NewService(application.ServiceConfig{
				Catalog: catalog, Store: store, NewID: id.NewString,
			})
			input := replayInput()
			first, err := service.CreateOrder(context.Background(), input)
			if err != nil {
				t.Fatal(err)
			}

			catalog.err = test.err
			catalog.price = 9900
			replay, err := service.CreateOrder(context.Background(), input)
			if err != nil {
				t.Fatalf("replay failed after catalog changed: %v", err)
			}
			if !replay.ReusedIdempotencyKey || !reflect.DeepEqual(replay.Order, first.Order) {
				t.Fatalf("replay = %#v, want original order %#v", replay, first.Order)
			}
			if catalog.calls != 1 {
				t.Fatalf("catalog calls = %d, want only the initial validation", catalog.calls)
			}
			if len(store.OutboxEvents()) != 1 {
				t.Fatal("replay created another outbox event")
			}
		})
	}
}

func TestCreateOrderConflictDoesNotDependOnCatalog(t *testing.T) {
	catalog := &changingCatalog{price: 1200}
	store := memory.NewStore()
	service := application.NewService(application.ServiceConfig{
		Catalog: catalog, Store: store, NewID: id.NewString,
	})
	input := replayInput()
	if _, err := service.CreateOrder(context.Background(), input); err != nil {
		t.Fatal(err)
	}

	catalog.err = application.ErrCatalogUnavailable
	input.Items[0].Quantity++
	_, err := service.CreateOrder(context.Background(), input)
	if !errors.Is(err, application.ErrIdempotencyConflict) {
		t.Fatalf("conflicting replay error = %v, want idempotency conflict", err)
	}
	if catalog.calls != 1 || len(store.OutboxEvents()) != 1 {
		t.Fatal("conflicting replay called catalog or created another event")
	}
}

func TestConcurrentCreateOrdersKeepSingleOrderAndOutboxEvent(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		name := "same payload"
		if conflict {
			name = "conflicting payload"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			store := memory.NewStore()
			catalog := &barrierCatalog{ready: make(chan struct{})}
			service := application.NewService(application.ServiceConfig{
				Catalog: catalog, Store: store, NewID: id.NewString,
			})
			type outcome struct {
				result application.CreateOrderResult
				err    error
			}
			results := make(chan outcome, 2)
			for i := 0; i < 2; i++ {
				input := replayInput()
				if conflict {
					input.Items[0].Quantity += int32(i)
				}
				go func() {
					result, err := service.CreateOrder(ctx, input)
					results <- outcome{result: result, err: err}
				}()
			}
			var outcomes []outcome
			for i := 0; i < 2; i++ {
				select {
				case result := <-results:
					outcomes = append(outcomes, result)
				case <-ctx.Done():
					t.Fatal("concurrent create timed out")
				}
			}
			created, reused, conflicts := 0, 0, 0
			for _, result := range outcomes {
				switch {
				case errors.Is(result.err, application.ErrIdempotencyConflict):
					conflicts++
				case result.err != nil:
					t.Fatalf("create failed: %v", result.err)
				case result.result.ReusedIdempotencyKey:
					reused++
				default:
					created++
				}
			}
			if created != 1 || len(store.OutboxEvents()) != 1 {
				t.Fatalf("created = %d, events = %d; want one of each", created, len(store.OutboxEvents()))
			}
			if conflict {
				if conflicts != 1 || reused != 0 {
					t.Fatalf("conflicts = %d, reused = %d", conflicts, reused)
				}
			} else if reused != 1 || conflicts != 0 || outcomes[0].result.Order.ID != outcomes[1].result.Order.ID {
				t.Fatalf("duplicate creates returned different orders: %#v", outcomes)
			}
		})
	}
}

func TestCreateOrderIdempotencyIsScopedToUser(t *testing.T) {
	store := memory.NewStore()
	service := application.NewService(application.ServiceConfig{
		Catalog: &changingCatalog{price: 1200}, Store: store, NewID: id.NewString,
	})
	first, err := service.CreateOrder(context.Background(), replayInput())
	if err != nil {
		t.Fatal(err)
	}
	input := replayInput()
	input.UserID = "usr-2"
	second, err := service.CreateOrder(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if second.ReusedIdempotencyKey || second.Order.ID == first.Order.ID || second.Order.UserID != "usr-2" {
		t.Fatalf("idempotency leaked across users: %#v", second)
	}
	if len(store.OutboxEvents()) != 2 {
		t.Fatal("separate users must create separate orders")
	}
}

type barrierCatalog struct {
	arrivals atomic.Int32
	ready    chan struct{}
}

func (c *barrierCatalog) ValidateOrderItems(ctx context.Context, _ string, items []application.RequestedItem) ([]orderdomain.NewOrderItemInput, error) {
	// Both requests must miss the initial lookup before either can persist.
	if c.arrivals.Add(1) == 2 {
		close(c.ready)
	}
	select {
	case <-c.ready:
		return []orderdomain.NewOrderItemInput{{
			MenuItemID: items[0].MenuItemID, Name: "Pizza", UnitPriceCents: 1200, Quantity: items[0].Quantity,
		}}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func replayInput() application.CreateOrderInput {
	return application.CreateOrderInput{
		UserID: "usr-1", IdempotencyKey: "replay-key", RestaurantID: "rst-1",
		Items: []application.RequestedItem{{MenuItemID: "pizza", Quantity: 1}},
	}
}

type changingCatalog struct {
	price int64
	err   error
	calls int
}

func (c *changingCatalog) ValidateOrderItems(_ context.Context, _ string, items []application.RequestedItem) ([]orderdomain.NewOrderItemInput, error) {
	c.calls++
	if c.err != nil {
		return nil, c.err
	}
	return []orderdomain.NewOrderItemInput{{
		MenuItemID: items[0].MenuItemID, Name: "Pizza",
		UnitPriceCents: c.price, Quantity: items[0].Quantity,
	}}, nil
}
