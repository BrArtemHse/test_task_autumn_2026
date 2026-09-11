package postgres_test

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/dispatch"
	partnerpostgres "github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/postgres"
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

func TestSubmissionStoreClaimsDuePendingRows(t *testing.T) {
	_, mock, store := newSubmissionStore(t)

	mock.ExpectBegin()
	mock.ExpectQuery(claimSubmissionsSQL).
		WithArgs("dispatcher-1", int64(30), int64(20)).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "order_id", "external_store_id", "destination_url", "payload", "attempts",
		}).AddRow(
			"sub-1",
			"ord-1",
			"store-pizza-1",
			"http://sample-restaurant-service:8084/orders",
			[]byte(`{"order_id":"ord-1"}`),
			int64(2),
		))
	mock.ExpectCommit()

	submissions, err := store.Claim(context.Background(), 20)
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	want := []dispatch.Submission{{
		ID:              "sub-1",
		OrderID:         "ord-1",
		ExternalStoreID: "store-pizza-1",
		DestinationURL:  "http://sample-restaurant-service:8084/orders",
		Payload:         []byte(`{"order_id":"ord-1"}`),
		Attempt:         2,
	}}
	if !reflect.DeepEqual(submissions, want) {
		t.Fatalf("submissions = %#v, want %#v", submissions, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet SQL expectations: %v", err)
	}
}

func TestSubmissionStoreMarksSubmittedOnlyWhenOwned(t *testing.T) {
	_, mock, store := newSubmissionStore(t)
	mock.ExpectExec(markSubmissionSubmittedSQL).
		WithArgs("sub-1", "dispatcher-1").
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := store.MarkSubmitted(context.Background(), "sub-1")
	if !errors.Is(err, partnerpostgres.ErrSubmissionNotOwned) {
		t.Fatalf("MarkSubmitted() error = %v, want %v", err, partnerpostgres.ErrSubmissionNotOwned)
	}
}

func TestSubmissionStoreMarksTransientAndTerminalFailures(t *testing.T) {
	tests := []struct {
		name     string
		terminal bool
	}{
		{name: "transient", terminal: false},
		{name: "terminal", terminal: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, mock, store := newSubmissionStore(t)
			mock.ExpectExec(markSubmissionFailedSQL).
				WithArgs("sub-1", "dispatcher-1", tt.terminal, "send failed", int64(5)).
				WillReturnResult(sqlmock.NewResult(0, 1))

			if err := store.MarkFailed(context.Background(), "sub-1", errors.New("send failed"), tt.terminal); err != nil {
				t.Fatalf("MarkFailed() error = %v", err)
			}
		})
	}
}

func newSubmissionStore(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *partnerpostgres.SubmissionStore) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := partnerpostgres.NewSubmissionStore(db, partnerpostgres.SubmissionConfig{
		DispatcherID: "dispatcher-1",
		LockTTL:      30 * time.Second,
		RetryDelay:   5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewSubmissionStore() error = %v", err)
	}
	return db, mock, store
}
