package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/application"
	orderdomain "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/order/domain"
)

const (
	advisoryLockSQL       = "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"
	selectIdempotencySQL  = "SELECT request_fingerprint, order_id FROM orders.idempotency_keys WHERE user_id = $1 AND idempotency_key = $2"
	insertOrderSQL        = "INSERT INTO orders.orders (id, user_id, restaurant_id, status, total_cents, version) VALUES ($1, $2, $3, $4, $5, $6)"
	insertOrderItemSQL    = "INSERT INTO orders.order_items (order_id, menu_item_id, name, unit_price_cents, quantity, modifier_item_ids, line_total_cents) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)"
	insertIdempotencySQL  = "INSERT INTO orders.idempotency_keys (user_id, idempotency_key, request_fingerprint, order_id) VALUES ($1, $2, $3, $4)"
	insertOutboxSQL       = "INSERT INTO orders.outbox_messages (id, event_type, event_version, producer, aggregate_type, aggregate_id, partition_key, correlation_id, causation_id, payload, occurred_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11)"
	selectOrderByIDSQL    = "SELECT id, user_id, restaurant_id, status, total_cents, version FROM orders.orders WHERE id = $1"
	selectOrderForUserSQL = "SELECT id, user_id, restaurant_id, status, total_cents, version FROM orders.orders WHERE id = $1 AND user_id = $2"
	selectOrderForUpdate  = "SELECT id, user_id, restaurant_id, status, total_cents, version FROM orders.orders WHERE id = $1 FOR UPDATE"
	selectOrderItemsSQL   = "SELECT menu_item_id, name, unit_price_cents, quantity, modifier_item_ids, line_total_cents FROM orders.order_items WHERE order_id = $1 ORDER BY id"
	selectInboxSQL        = "SELECT payload_fingerprint, aggregate_id FROM orders.inbox_messages WHERE event_id = $1"
	updateOrderStatusSQL  = "UPDATE orders.orders SET status = $2, version = $3, updated_at = now() WHERE id = $1"
	insertInboxSQL        = "INSERT INTO orders.inbox_messages (event_id, event_type, event_version, aggregate_id, payload_fingerprint) VALUES ($1, $2, $3, $4, $5)"
)

type Store struct {
	db *sql.DB
}

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type idempotencyRecord struct {
	fingerprint string
	orderID     string
}

type inboxRecord struct {
	fingerprint string
	orderID     string
}

func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

func (s *Store) FindOrderByIdempotencyKey(ctx context.Context, userID, key, fingerprint string) (application.CreateOrderResult, bool, error) {
	if s.db == nil {
		return application.CreateOrderResult{}, false, errors.New("postgres order store requires database")
	}
	return findOrderByIdempotencyKey(ctx, s.db, userID, key, fingerprint)
}

func (s *Store) CreateOrder(ctx context.Context, record application.CreateOrderRecord) (application.CreateOrderResult, error) {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return application.CreateOrderResult{}, err
	}
	committed := false
	defer rollbackUnlessCommitted(tx, &committed)

	if err := acquireAdvisoryLock(ctx, tx, record.Order.UserID+":"+record.IdempotencyKey); err != nil {
		return application.CreateOrderResult{}, err
	}

	// Recheck under the lock: concurrent requests may both miss the initial lookup.
	existing, found, err := findOrderByIdempotencyKey(ctx, tx, record.Order.UserID, record.IdempotencyKey, record.RequestFingerprint)
	if err != nil {
		return application.CreateOrderResult{}, err
	}
	if found {
		if err := tx.Commit(); err != nil {
			return application.CreateOrderResult{}, err
		}
		committed = true
		return existing, nil
	}

	if err := insertOrder(ctx, tx, record.Order); err != nil {
		return application.CreateOrderResult{}, err
	}
	if err := insertOrderItems(ctx, tx, record.Order); err != nil {
		return application.CreateOrderResult{}, err
	}
	if _, err := tx.ExecContext(ctx, insertIdempotencySQL, record.Order.UserID, record.IdempotencyKey, record.RequestFingerprint, record.Order.ID); err != nil {
		return application.CreateOrderResult{}, err
	}
	if err := insertOutboxEvent(ctx, tx, record); err != nil {
		return application.CreateOrderResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return application.CreateOrderResult{}, err
	}
	committed = true

	return application.CreateOrderResult{Order: copyOrder(record.Order)}, nil
}

func (s *Store) GetOrder(ctx context.Context, orderID, userID string) (orderdomain.Order, error) {
	if s.db == nil {
		return orderdomain.Order{}, fmt.Errorf("%w: database is required", application.ErrOrderNotFound)
	}

	order, err := loadOrderForUser(ctx, s.db, orderID, userID)
	if err != nil {
		return orderdomain.Order{}, err
	}
	return order, nil
}

func (s *Store) ApplyPartnerEvent(ctx context.Context, record application.ApplyPartnerEventRecord) (application.ApplyPartnerEventResult, error) {
	tx, err := s.beginTx(ctx)
	if err != nil {
		return application.ApplyPartnerEventResult{}, err
	}
	committed := false
	defer rollbackUnlessCommitted(tx, &committed)

	if err := acquireAdvisoryLock(ctx, tx, record.EventID); err != nil {
		return application.ApplyPartnerEventResult{}, err
	}

	existing, found, err := lookupInbox(ctx, tx, record.EventID)
	if err != nil {
		return application.ApplyPartnerEventResult{}, err
	}
	if found {
		if existing.fingerprint != record.PayloadFingerprint {
			return application.ApplyPartnerEventResult{}, application.ErrInboxConflict
		}

		order, err := loadOrderByID(ctx, tx, existing.orderID)
		if err != nil {
			return application.ApplyPartnerEventResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return application.ApplyPartnerEventResult{}, err
		}
		committed = true
		return application.ApplyPartnerEventResult{
			Order:     order,
			Duplicate: true,
		}, nil
	}

	order, err := loadOrderForUpdate(ctx, tx, record.OrderID)
	if err != nil {
		return application.ApplyPartnerEventResult{}, err
	}

	changed, err := order.TransitionTo(record.NewStatus)
	if err != nil {
		return application.ApplyPartnerEventResult{}, err
	}
	if changed {
		if _, err := tx.ExecContext(ctx, updateOrderStatusSQL, order.ID, string(order.Status), int64(order.Version)); err != nil {
			return application.ApplyPartnerEventResult{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, insertInboxSQL, record.EventID, record.EventType, int64(1), record.OrderID, record.PayloadFingerprint); err != nil {
		return application.ApplyPartnerEventResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return application.ApplyPartnerEventResult{}, err
	}
	committed = true

	return application.ApplyPartnerEventResult{
		Order:   order,
		Changed: changed,
	}, nil
}

func (s *Store) beginTx(ctx context.Context) (*sql.Tx, error) {
	if s.db == nil {
		return nil, errors.New("postgres order store requires database")
	}
	return s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}

func rollbackUnlessCommitted(tx *sql.Tx, committed *bool) {
	if !*committed {
		_ = tx.Rollback()
	}
}

func acquireAdvisoryLock(ctx context.Context, tx *sql.Tx, key string) error {
	_, err := tx.ExecContext(ctx, advisoryLockSQL, key)
	return err
}

func findOrderByIdempotencyKey(ctx context.Context, q queryer, userID, key, fingerprint string) (application.CreateOrderResult, bool, error) {
	record, found, err := lookupIdempotency(ctx, q, userID, key)
	if err != nil || !found {
		return application.CreateOrderResult{}, found, err
	}
	if record.fingerprint != fingerprint {
		return application.CreateOrderResult{}, true, application.ErrIdempotencyConflict
	}
	order, err := loadOrderByID(ctx, q, record.orderID)
	if err != nil {
		return application.CreateOrderResult{}, true, err
	}
	return application.CreateOrderResult{Order: order, ReusedIdempotencyKey: true}, true, nil
}

func lookupIdempotency(ctx context.Context, q queryer, userID, idempotencyKey string) (idempotencyRecord, bool, error) {
	var record idempotencyRecord
	err := q.QueryRowContext(ctx, selectIdempotencySQL, userID, idempotencyKey).Scan(&record.fingerprint, &record.orderID)
	if errors.Is(err, sql.ErrNoRows) {
		return idempotencyRecord{}, false, nil
	}
	if err != nil {
		return idempotencyRecord{}, false, err
	}
	return record, true, nil
}

func lookupInbox(ctx context.Context, q queryer, eventID string) (inboxRecord, bool, error) {
	var record inboxRecord
	err := q.QueryRowContext(ctx, selectInboxSQL, eventID).Scan(&record.fingerprint, &record.orderID)
	if errors.Is(err, sql.ErrNoRows) {
		return inboxRecord{}, false, nil
	}
	if err != nil {
		return inboxRecord{}, false, err
	}
	return record, true, nil
}

func insertOrder(ctx context.Context, tx *sql.Tx, order orderdomain.Order) error {
	_, err := tx.ExecContext(
		ctx,
		insertOrderSQL,
		order.ID,
		order.UserID,
		order.RestaurantID,
		string(order.Status),
		order.TotalCents,
		int64(order.Version),
	)
	return err
}

func insertOrderItems(ctx context.Context, tx *sql.Tx, order orderdomain.Order) error {
	for _, item := range order.Items {
		modifiers, err := json.Marshal(item.ModifierItemIDs)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(
			ctx,
			insertOrderItemSQL,
			order.ID,
			item.MenuItemID,
			item.Name,
			item.UnitPriceCents,
			int64(item.Quantity),
			string(modifiers),
			item.LineTotalCents,
		); err != nil {
			return err
		}
	}
	return nil
}

func insertOutboxEvent(ctx context.Context, tx *sql.Tx, record application.CreateOrderRecord) error {
	event := record.OutboxEvent
	_, err := tx.ExecContext(
		ctx,
		insertOutboxSQL,
		event.EventID,
		event.EventType,
		int64(event.EventVersion),
		event.Producer,
		event.AggregateType,
		event.AggregateID,
		event.PartitionKey,
		event.CorrelationID,
		event.CausationID,
		string(event.Payload),
		event.OccurredAt,
	)
	return err
}

func loadOrderForUser(ctx context.Context, q queryer, orderID, userID string) (orderdomain.Order, error) {
	return loadOrderFromRow(ctx, q, q.QueryRowContext(ctx, selectOrderForUserSQL, orderID, userID))
}

func loadOrderByID(ctx context.Context, q queryer, orderID string) (orderdomain.Order, error) {
	return loadOrderFromRow(ctx, q, q.QueryRowContext(ctx, selectOrderByIDSQL, orderID))
}

func loadOrderForUpdate(ctx context.Context, q queryer, orderID string) (orderdomain.Order, error) {
	return loadOrderFromRow(ctx, q, q.QueryRowContext(ctx, selectOrderForUpdate, orderID))
}

func loadOrderFromRow(ctx context.Context, q queryer, row *sql.Row) (orderdomain.Order, error) {
	var order orderdomain.Order
	var status string
	var version int64
	if err := row.Scan(&order.ID, &order.UserID, &order.RestaurantID, &status, &order.TotalCents, &version); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return orderdomain.Order{}, application.ErrOrderNotFound
		}
		return orderdomain.Order{}, err
	}

	order.Status = orderdomain.Status(status)
	order.Version = int(version)

	items, err := loadOrderItems(ctx, q, order.ID)
	if err != nil {
		return orderdomain.Order{}, err
	}
	order.Items = items

	return order, nil
}

func loadOrderItems(ctx context.Context, q queryer, orderID string) (items []orderdomain.OrderItem, err error) {
	rows, err := q.QueryContext(ctx, selectOrderItemsSQL, orderID)
	if err != nil {
		return nil, err
	}
	defer func() {
		closeErr := rows.Close()
		if err == nil && closeErr != nil {
			err = closeErr
		}
	}()

	for rows.Next() {
		var item orderdomain.OrderItem
		var quantity int64
		var modifiersJSON []byte
		if err := rows.Scan(
			&item.MenuItemID,
			&item.Name,
			&item.UnitPriceCents,
			&quantity,
			&modifiersJSON,
			&item.LineTotalCents,
		); err != nil {
			return nil, err
		}
		item.Quantity = int32(quantity)
		if err := json.Unmarshal(modifiersJSON, &item.ModifierItemIDs); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, nil
}

func copyOrder(order orderdomain.Order) orderdomain.Order {
	order.Items = append([]orderdomain.OrderItem(nil), order.Items...)
	for i := range order.Items {
		order.Items[i].ModifierItemIDs = append([]string(nil), order.Items[i].ModifierItemIDs...)
	}
	return order
}
