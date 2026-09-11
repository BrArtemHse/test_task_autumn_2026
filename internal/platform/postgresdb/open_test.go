package postgresdb

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestOpenRejectsEmptyDatabaseURL(t *testing.T) {
	db, err := Open(context.Background(), "")
	if db != nil {
		t.Fatal("db is non-nil, want nil")
	}
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Open() error = %v, want %v", err, ErrInvalidConfig)
	}
}

func TestWaitForReadyRetriesUntilPingSucceeds(t *testing.T) {
	attempts := 0
	pingErr := errors.New("connection refused")
	pinger := pingerFunc(func(context.Context) error {
		attempts++
		if attempts < 3 {
			return pingErr
		}
		return nil
	})

	err := waitForReady(context.Background(), pinger, time.Millisecond)
	if err != nil {
		t.Fatalf("waitForReady() error = %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestWaitForReadyStopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pingErr := errors.New("connection refused")
	pinger := pingerFunc(func(context.Context) error {
		cancel()
		return pingErr
	})

	err := waitForReady(ctx, pinger, time.Hour)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForReady() error = %v, want context cancellation", err)
	}
	if !errors.Is(err, pingErr) {
		t.Fatalf("waitForReady() error = %v, want last ping error", err)
	}
}

type pingerFunc func(context.Context) error

func (fn pingerFunc) PingContext(ctx context.Context) error {
	return fn(ctx)
}
