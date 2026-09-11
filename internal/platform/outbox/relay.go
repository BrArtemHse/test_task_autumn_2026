package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrInvalidConfig = errors.New("invalid outbox relay config")

type Message struct {
	EventID string
	Topic   string
	Key     string
	Value   []byte
	Headers map[string]string
}

type Store interface {
	Claim(ctx context.Context, limit int) ([]Message, error)
	MarkPublished(ctx context.Context, eventID string) error
	MarkFailed(ctx context.Context, eventID string, cause error) error
}

type Publisher interface {
	Publish(ctx context.Context, message Message) error
}

type Config struct {
	BatchSize    int
	PollInterval time.Duration
	Store        Store
	Publisher    Publisher
	ErrorHandler func(error)
}

type Relay struct {
	batchSize    int
	pollInterval time.Duration
	store        Store
	publisher    Publisher
	errorHandler func(error)
}

func NewRelay(config Config) (*Relay, error) {
	if config.BatchSize <= 0 {
		return nil, fmt.Errorf("%w: batch size must be positive", ErrInvalidConfig)
	}
	if config.PollInterval <= 0 {
		return nil, fmt.Errorf("%w: poll interval must be positive", ErrInvalidConfig)
	}
	if config.Store == nil {
		return nil, fmt.Errorf("%w: store is required", ErrInvalidConfig)
	}
	if config.Publisher == nil {
		return nil, fmt.Errorf("%w: publisher is required", ErrInvalidConfig)
	}

	return &Relay{
		batchSize:    config.BatchSize,
		pollInterval: config.PollInterval,
		store:        config.Store,
		publisher:    config.Publisher,
		errorHandler: config.ErrorHandler,
	}, nil
}

func (r *Relay) RunOnce(ctx context.Context) error {
	messages, err := r.store.Claim(ctx, r.batchSize)
	if err != nil {
		return err
	}

	for _, message := range messages {
		if err := r.publisher.Publish(ctx, copyMessage(message)); err != nil {
			if markErr := r.store.MarkFailed(ctx, message.EventID, err); markErr != nil {
				return markErr
			}
			continue
		}
		if err := r.store.MarkPublished(ctx, message.EventID); err != nil {
			return err
		}
	}

	return nil
}

func (r *Relay) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := r.RunOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.reportError(err)
		}

		timer := time.NewTimer(r.pollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (r *Relay) reportError(err error) {
	if r.errorHandler != nil {
		r.errorHandler(err)
	}
}

func copyMessage(message Message) Message {
	message.Value = append([]byte(nil), message.Value...)
	if message.Headers != nil {
		headers := make(map[string]string, len(message.Headers))
		for key, value := range message.Headers {
			headers[key] = value
		}
		message.Headers = headers
	}
	return message
}
