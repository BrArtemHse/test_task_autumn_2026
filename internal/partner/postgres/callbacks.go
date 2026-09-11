package postgres

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/callback"
)

const (
	selectCallbackIntegrationSQL = "SELECT shared_secret_hash FROM partner.restaurant_integrations WHERE restaurant_id = $1 AND external_store_id = $2 AND status = 'active' FOR SHARE"
	selectCallbackSubmissionSQL  = "SELECT status FROM partner.partner_order_submissions WHERE order_id = $1 AND restaurant_id = $2 AND external_store_id = $3 FOR UPDATE"
	selectCallbackSQL            = "SELECT order_id, external_store_id, payload_fingerprint FROM partner.partner_callbacks WHERE id = $1"
	insertCallbackSQL            = "INSERT INTO partner.partner_callbacks (id, order_id, external_store_id, callback_type, payload, payload_fingerprint, received_at) VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)"
	insertCallbackOutboxSQL      = "INSERT INTO partner.outbox_messages (id, event_type, event_version, producer, aggregate_type, aggregate_id, partition_key, correlation_id, causation_id, payload, occurred_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11)"
	updateCallbackSubmissionSQL  = "UPDATE partner.partner_order_submissions SET status = $4, updated_at = now() WHERE order_id = $1 AND restaurant_id = $2 AND external_store_id = $3"
)

type CallbackStore struct {
	db *sql.DB
}

type callbackRecord struct {
	orderID            string
	externalStoreID    string
	payloadFingerprint string
}

func NewCallbackStore(db *sql.DB) *CallbackStore {
	return &CallbackStore{db: db}
}

func (s *CallbackStore) ReceiveStatusCallback(ctx context.Context, record callback.ReceiveRecord) (callback.ReceiveResult, error) {
	if s.db == nil {
		return callback.ReceiveResult{}, errors.New("partner callback store requires database")
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return callback.ReceiveResult{}, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if _, err := tx.ExecContext(ctx, advisoryLockSQL, record.CallbackID); err != nil {
		return callback.ReceiveResult{}, err
	}
	if err := authorizeCallback(ctx, tx, record); err != nil {
		return callback.ReceiveResult{}, err
	}
	if err := authorizeCallbackSubmission(ctx, tx, record); err != nil {
		return callback.ReceiveResult{}, err
	}

	existing, found, err := lookupCallback(ctx, tx, record.CallbackID)
	if err != nil {
		return callback.ReceiveResult{}, err
	}
	if found {
		if existing.orderID != record.Payload.OrderID ||
			existing.externalStoreID != record.Payload.ExternalStoreID ||
			existing.payloadFingerprint != record.PayloadFingerprint {
			return callback.ReceiveResult{}, callback.ErrCallbackConflict
		}
		if err := tx.Commit(); err != nil {
			return callback.ReceiveResult{}, err
		}
		committed = true
		return callback.ReceiveResult{Duplicate: true}, nil
	}

	payload, err := json.Marshal(record.Payload)
	if err != nil {
		return callback.ReceiveResult{}, err
	}
	if _, err := tx.ExecContext(
		ctx,
		insertCallbackSQL,
		record.CallbackID,
		record.Payload.OrderID,
		record.Payload.ExternalStoreID,
		record.Payload.Status,
		string(payload),
		record.PayloadFingerprint,
		record.OutboxEvent.OccurredAt,
	); err != nil {
		return callback.ReceiveResult{}, err
	}
	event := record.OutboxEvent
	if _, err := tx.ExecContext(
		ctx,
		insertCallbackOutboxSQL,
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
	); err != nil {
		return callback.ReceiveResult{}, err
	}
	if _, err := tx.ExecContext(
		ctx,
		updateCallbackSubmissionSQL,
		record.Payload.OrderID,
		record.Payload.RestaurantID,
		record.Payload.ExternalStoreID,
		record.Payload.Status,
	); err != nil {
		return callback.ReceiveResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return callback.ReceiveResult{}, err
	}
	committed = true
	return callback.ReceiveResult{}, nil
}

func authorizeCallbackSubmission(ctx context.Context, tx *sql.Tx, record callback.ReceiveRecord) error {
	var status string
	err := tx.QueryRowContext(
		ctx,
		selectCallbackSubmissionSQL,
		record.Payload.OrderID,
		record.Payload.RestaurantID,
		record.Payload.ExternalStoreID,
	).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return callback.ErrUnauthorizedCallback
	}
	if err != nil {
		return err
	}
	if (status == "accepted" || status == "rejected") && status != record.Payload.Status {
		return callback.ErrCallbackConflict
	}
	return nil
}

func authorizeCallback(ctx context.Context, tx *sql.Tx, record callback.ReceiveRecord) error {
	var storedDigest string
	err := tx.QueryRowContext(
		ctx,
		selectCallbackIntegrationSQL,
		record.Payload.RestaurantID,
		record.Payload.ExternalStoreID,
	).Scan(&storedDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return callback.ErrUnauthorizedCallback
	}
	if err != nil {
		return err
	}
	if subtle.ConstantTimeCompare([]byte(storedDigest), []byte(record.TokenDigest)) != 1 {
		return callback.ErrUnauthorizedCallback
	}
	return nil
}

func lookupCallback(ctx context.Context, tx *sql.Tx, callbackID string) (callbackRecord, bool, error) {
	var record callbackRecord
	err := tx.QueryRowContext(ctx, selectCallbackSQL, callbackID).Scan(
		&record.orderID,
		&record.externalStoreID,
		&record.payloadFingerprint,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return callbackRecord{}, false, nil
	}
	if err != nil {
		return callbackRecord{}, false, err
	}
	return record, true, nil
}
