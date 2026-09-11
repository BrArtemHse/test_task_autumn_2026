package postgresdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var ErrInvalidConfig = errors.New("invalid postgres config")

const (
	defaultMaxOpenConns    = 10
	defaultMaxIdleConns    = 5
	defaultConnMaxLifetime = 30 * time.Minute
	defaultPingTimeout     = 5 * time.Second
	defaultReadyTimeout    = 30 * time.Second
	defaultRetryDelay      = 500 * time.Millisecond
	defaultMaxRetryDelay   = 2 * time.Second
)

type pinger interface {
	PingContext(context.Context) error
}

func Open(ctx context.Context, databaseURL string) (*sql.DB, error) {
	if strings.TrimSpace(databaseURL) == "" {
		return nil, fmt.Errorf("%w: database url is required", ErrInvalidConfig)
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(defaultMaxOpenConns)
	db.SetMaxIdleConns(defaultMaxIdleConns)
	db.SetConnMaxLifetime(defaultConnMaxLifetime)

	readyCtx, cancel := context.WithTimeout(ctx, defaultReadyTimeout)
	defer cancel()
	if err := waitForReady(readyCtx, db, defaultRetryDelay); err != nil {
		closeErr := db.Close()
		if closeErr != nil {
			return nil, fmt.Errorf("wait for postgres: %w; close postgres: %v", err, closeErr)
		}
		return nil, err
	}

	return db, nil
}

func waitForReady(ctx context.Context, target pinger, initialRetryDelay time.Duration) error {
	retryDelay := min(initialRetryDelay, defaultMaxRetryDelay)
	for {
		pingCtx, cancel := context.WithTimeout(ctx, defaultPingTimeout)
		err := target.PingContext(pingCtx)
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("postgres readiness: %w", errors.Join(err, ctx.Err()))
		}

		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("postgres readiness: %w", errors.Join(err, ctx.Err()))
		case <-timer.C:
		}

		if retryDelay < defaultMaxRetryDelay {
			retryDelay = min(retryDelay*2, defaultMaxRetryDelay)
		}
	}
}
