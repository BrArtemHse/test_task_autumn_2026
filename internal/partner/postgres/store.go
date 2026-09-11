package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	partnerapp "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/application"
)

const (
	advisoryLockSQL      = "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"
	selectInboxSQL       = "SELECT payload_fingerprint, aggregate_id FROM partner.inbox_messages WHERE event_id = $1"
	selectIntegrationSQL = "SELECT external_store_id, callback_url FROM partner.restaurant_integrations WHERE restaurant_id = $1 AND status = 'active'"
	insertSubmissionSQL  = "INSERT INTO partner.partner_order_submissions (id, order_id, restaurant_id, external_store_id, destination_url, status, payload, correlation_id, source_event_id) VALUES ($1, $2, $3, $4, $5, 'pending', $6::jsonb, $7, $8)"
	insertInboxSQL       = "INSERT INTO partner.inbox_messages (event_id, event_type, event_version, aggregate_id, payload_fingerprint) VALUES ($1, $2, $3, $4, $5)"
)

type Store struct {
	db *sql.DB
}

type inboxRecord struct {
	fingerprint string
	aggregateID string
}

type integrationRecord struct {
	externalStoreID string
	destinationURL  string
}

func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

func (s *Store) ReceiveOrder(ctx context.Context, record partnerapp.ReceiveOrderRecord) (partnerapp.ReceiveOrderResult, error) {
	if s.db == nil {
		return partnerapp.ReceiveOrderResult{}, errors.New("partner postgres store requires database")
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return partnerapp.ReceiveOrderResult{}, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if _, err := tx.ExecContext(ctx, advisoryLockSQL, record.EventID); err != nil {
		return partnerapp.ReceiveOrderResult{}, err
	}

	existing, found, err := lookupInbox(ctx, tx, record.EventID)
	if err != nil {
		return partnerapp.ReceiveOrderResult{}, err
	}
	if found {
		if existing.fingerprint != record.PayloadFingerprint || existing.aggregateID != record.OrderID {
			return partnerapp.ReceiveOrderResult{}, partnerapp.ErrInboxConflict
		}
		if err := tx.Commit(); err != nil {
			return partnerapp.ReceiveOrderResult{}, err
		}
		committed = true
		return partnerapp.ReceiveOrderResult{Duplicate: true}, nil
	}

	integration, err := lookupIntegration(ctx, tx, record.RestaurantID)
	if err != nil {
		return partnerapp.ReceiveOrderResult{}, err
	}
	payload, err := json.Marshal(record.Payload)
	if err != nil {
		return partnerapp.ReceiveOrderResult{}, err
	}
	if _, err := tx.ExecContext(
		ctx,
		insertSubmissionSQL,
		record.SubmissionID,
		record.OrderID,
		record.RestaurantID,
		integration.externalStoreID,
		integration.destinationURL,
		string(payload),
		record.CorrelationID,
		record.EventID,
	); err != nil {
		return partnerapp.ReceiveOrderResult{}, err
	}
	if _, err := tx.ExecContext(
		ctx,
		insertInboxSQL,
		record.EventID,
		record.EventType,
		int64(record.EventVersion),
		record.OrderID,
		record.PayloadFingerprint,
	); err != nil {
		return partnerapp.ReceiveOrderResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return partnerapp.ReceiveOrderResult{}, err
	}
	committed = true

	return partnerapp.ReceiveOrderResult{SubmissionID: record.SubmissionID}, nil
}

func lookupInbox(ctx context.Context, tx *sql.Tx, eventID string) (inboxRecord, bool, error) {
	var record inboxRecord
	err := tx.QueryRowContext(ctx, selectInboxSQL, eventID).Scan(&record.fingerprint, &record.aggregateID)
	if errors.Is(err, sql.ErrNoRows) {
		return inboxRecord{}, false, nil
	}
	if err != nil {
		return inboxRecord{}, false, err
	}
	return record, true, nil
}

func lookupIntegration(ctx context.Context, tx *sql.Tx, restaurantID string) (integrationRecord, error) {
	var record integrationRecord
	err := tx.QueryRowContext(ctx, selectIntegrationSQL, restaurantID).Scan(
		&record.externalStoreID,
		&record.destinationURL,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return integrationRecord{}, fmt.Errorf("%w: restaurant %s", partnerapp.ErrIntegrationNotFound, restaurantID)
	}
	if err != nil {
		return integrationRecord{}, err
	}
	return record, nil
}
