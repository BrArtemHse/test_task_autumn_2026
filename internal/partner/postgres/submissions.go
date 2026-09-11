package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/dispatch"
)

const (
	claimSubmissionsSQL = `UPDATE partner.partner_order_submissions
SET locked_by = $1,
    locked_until = now() + ($2 * interval '1 second'),
    attempts = attempts + 1,
    last_error = NULL
WHERE id IN (
    SELECT id
    FROM partner.partner_order_submissions
    WHERE status = 'pending'
      AND (locked_until IS NULL OR locked_until < now())
      AND (next_attempt_at IS NULL OR next_attempt_at <= now())
    ORDER BY created_at, id
    LIMIT $3
    FOR UPDATE SKIP LOCKED
)
RETURNING id, order_id, external_store_id, destination_url, payload::text, attempts`
	markSubmissionSubmittedSQL = `UPDATE partner.partner_order_submissions
SET status = 'submitted',
    submitted_at = now(),
    locked_by = NULL,
    locked_until = NULL,
    next_attempt_at = NULL,
    last_error = NULL
WHERE id = $1
  AND locked_by = $2
  AND status = 'pending'`
	markSubmissionFailedSQL = `UPDATE partner.partner_order_submissions
SET status = CASE WHEN $3 THEN 'failed' ELSE 'pending' END,
    locked_by = NULL,
    locked_until = NULL,
    last_error = $4,
    next_attempt_at = CASE
        WHEN $3 THEN NULL
        ELSE now() + ($5 * interval '1 second')
    END
WHERE id = $1
  AND locked_by = $2
  AND status = 'pending'`
)

var (
	ErrInvalidSubmissionConfig = errors.New("invalid partner submission store config")
	ErrSubmissionNotOwned      = errors.New("partner submission is not owned by this dispatcher")
)

type SubmissionConfig struct {
	DispatcherID string
	LockTTL      time.Duration
	RetryDelay   time.Duration
}

type SubmissionStore struct {
	db           *sql.DB
	dispatcherID string
	lockTTL      time.Duration
	retryDelay   time.Duration
}

func NewSubmissionStore(db *sql.DB, config SubmissionConfig) (*SubmissionStore, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: database is required", ErrInvalidSubmissionConfig)
	}
	dispatcherID := strings.TrimSpace(config.DispatcherID)
	if dispatcherID == "" || config.LockTTL < time.Second || config.RetryDelay < time.Second {
		return nil, ErrInvalidSubmissionConfig
	}
	return &SubmissionStore{
		db:           db,
		dispatcherID: dispatcherID,
		lockTTL:      config.LockTTL,
		retryDelay:   config.RetryDelay,
	}, nil
}

func (s *SubmissionStore) Claim(ctx context.Context, limit int) ([]dispatch.Submission, error) {
	if limit <= 0 {
		return nil, ErrInvalidSubmissionConfig
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
		claimSubmissionsSQL,
		s.dispatcherID,
		int64(s.lockTTL/time.Second),
		int64(limit),
	)
	if err != nil {
		return nil, err
	}
	var submissions []dispatch.Submission
	for rows.Next() {
		var submission dispatch.Submission
		var attempts int64
		if err := rows.Scan(
			&submission.ID,
			&submission.OrderID,
			&submission.ExternalStoreID,
			&submission.DestinationURL,
			&submission.Payload,
			&attempts,
		); err != nil {
			_ = rows.Close()
			return nil, err
		}
		submission.Attempt = int(attempts)
		submissions = append(submissions, submission)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	committed = true
	return submissions, nil
}

func (s *SubmissionStore) MarkSubmitted(ctx context.Context, submissionID string) error {
	result, err := s.db.ExecContext(ctx, markSubmissionSubmittedSQL, submissionID, s.dispatcherID)
	if err != nil {
		return err
	}
	return requireOwnedSubmission(result)
}

func (s *SubmissionStore) MarkFailed(ctx context.Context, submissionID string, cause error, terminal bool) error {
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	result, err := s.db.ExecContext(
		ctx,
		markSubmissionFailedSQL,
		submissionID,
		s.dispatcherID,
		terminal,
		message,
		int64(s.retryDelay/time.Second),
	)
	if err != nil {
		return err
	}
	return requireOwnedSubmission(result)
}

func requireOwnedSubmission(result sql.Result) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrSubmissionNotOwned
	}
	return nil
}
