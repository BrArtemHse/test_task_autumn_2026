package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/outbox"
)

const (
	claimPartnerOutboxSQL = `UPDATE partner.outbox_messages
SET locked_by = $1,
    locked_until = now() + ($2 * interval '1 second'),
    publish_attempts = publish_attempts + 1,
    last_error = NULL
WHERE id IN (
    SELECT id
    FROM partner.outbox_messages
    WHERE published_at IS NULL
      AND (locked_until IS NULL OR locked_until < now())
      AND (next_attempt_at IS NULL OR next_attempt_at <= now())
    ORDER BY occurred_at, id
    LIMIT $3
    FOR UPDATE SKIP LOCKED
)
RETURNING id, event_type, event_version, producer, aggregate_type, aggregate_id, partition_key, correlation_id, causation_id, payload::text, occurred_at`
	markPartnerOutboxPublishedSQL = `UPDATE partner.outbox_messages
SET published_at = now(),
    locked_by = NULL,
    locked_until = NULL,
    next_attempt_at = NULL,
    last_error = NULL
WHERE id = $1
  AND locked_by = $2
  AND published_at IS NULL`
	markPartnerOutboxFailedSQL = `UPDATE partner.outbox_messages
SET locked_by = NULL,
    locked_until = NULL,
    last_error = $3,
    next_attempt_at = now() + ($4 * interval '1 second')
WHERE id = $1
  AND locked_by = $2
  AND published_at IS NULL`
)

var (
	ErrInvalidPartnerOutboxConfig   = errors.New("invalid partner outbox config")
	ErrPartnerOutboxMessageNotOwned = errors.New("partner outbox message is not owned by this relay")
)

type OutboxConfig struct {
	RelayID    string
	Topic      string
	LockTTL    time.Duration
	RetryDelay time.Duration
}

type OutboxStore struct {
	db         *sql.DB
	relayID    string
	topic      string
	lockTTL    time.Duration
	retryDelay time.Duration
}

type partnerOutboxRow struct {
	eventID       string
	eventType     string
	eventVersion  int64
	producer      string
	aggregateType string
	aggregateID   string
	partitionKey  string
	correlationID string
	causationID   string
	payload       []byte
	occurredAt    time.Time
}

func NewOutboxStore(db *sql.DB, config OutboxConfig) (*OutboxStore, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: database is required", ErrInvalidPartnerOutboxConfig)
	}
	relayID := strings.TrimSpace(config.RelayID)
	if relayID == "" {
		return nil, fmt.Errorf("%w: relay id is required", ErrInvalidPartnerOutboxConfig)
	}
	topic := strings.TrimSpace(config.Topic)
	if topic == "" {
		return nil, fmt.Errorf("%w: topic is required", ErrInvalidPartnerOutboxConfig)
	}
	if config.LockTTL < time.Second {
		return nil, fmt.Errorf("%w: lock ttl must be at least one second", ErrInvalidPartnerOutboxConfig)
	}
	if config.RetryDelay < time.Second {
		return nil, fmt.Errorf("%w: retry delay must be at least one second", ErrInvalidPartnerOutboxConfig)
	}
	return &OutboxStore{
		db:         db,
		relayID:    relayID,
		topic:      topic,
		lockTTL:    config.LockTTL,
		retryDelay: config.RetryDelay,
	}, nil
}

func (s *OutboxStore) Claim(ctx context.Context, limit int) ([]outbox.Message, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("%w: claim limit must be positive", ErrInvalidPartnerOutboxConfig)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	rows, err := tx.QueryContext(
		ctx,
		claimPartnerOutboxSQL,
		s.relayID,
		partnerOutboxDurationSeconds(s.lockTTL),
		int64(limit),
	)
	if err != nil {
		return nil, err
	}
	messages, err := s.scanMessages(rows)
	if closeErr := rows.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	committed = true
	return messages, nil
}

func (s *OutboxStore) MarkPublished(ctx context.Context, eventID string) error {
	result, err := s.db.ExecContext(ctx, markPartnerOutboxPublishedSQL, eventID, s.relayID)
	if err != nil {
		return err
	}
	return requireOwnedPartnerOutboxRow(result)
}

func (s *OutboxStore) MarkFailed(ctx context.Context, eventID string, cause error) error {
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	result, err := s.db.ExecContext(
		ctx,
		markPartnerOutboxFailedSQL,
		eventID,
		s.relayID,
		message,
		partnerOutboxDurationSeconds(s.retryDelay),
	)
	if err != nil {
		return err
	}
	return requireOwnedPartnerOutboxRow(result)
}

func (s *OutboxStore) scanMessages(rows *sql.Rows) ([]outbox.Message, error) {
	var messages []outbox.Message
	for rows.Next() {
		var row partnerOutboxRow
		if err := rows.Scan(
			&row.eventID,
			&row.eventType,
			&row.eventVersion,
			&row.producer,
			&row.aggregateType,
			&row.aggregateID,
			&row.partitionKey,
			&row.correlationID,
			&row.causationID,
			&row.payload,
			&row.occurredAt,
		); err != nil {
			return nil, err
		}
		messages = append(messages, s.messageFromRow(row))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return messages, nil
}

func (s *OutboxStore) messageFromRow(row partnerOutboxRow) outbox.Message {
	return outbox.Message{
		EventID: row.eventID,
		Topic:   s.topic,
		Key:     row.partitionKey,
		Value:   append([]byte(nil), row.payload...),
		Headers: map[string]string{
			"aggregate_id":   row.aggregateID,
			"aggregate_type": row.aggregateType,
			"causation_id":   row.causationID,
			"correlation_id": row.correlationID,
			"event_id":       row.eventID,
			"event_type":     row.eventType,
			"event_version":  strconv.FormatInt(row.eventVersion, 10),
			"occurred_at":    row.occurredAt.Format(time.RFC3339Nano),
			"producer":       row.producer,
		},
	}
}

func requireOwnedPartnerOutboxRow(result sql.Result) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrPartnerOutboxMessageNotOwned
	}
	return nil
}

func partnerOutboxDurationSeconds(duration time.Duration) int64 {
	return int64(duration / time.Second)
}
