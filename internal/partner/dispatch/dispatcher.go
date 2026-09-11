package dispatch

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrInvalidConfig = errors.New("invalid partner dispatcher config")

type Submission struct {
	ID              string
	OrderID         string
	ExternalStoreID string
	DestinationURL  string
	Payload         []byte
	Attempt         int
}

type Store interface {
	Claim(ctx context.Context, limit int) ([]Submission, error)
	MarkSubmitted(ctx context.Context, submissionID string) error
	MarkFailed(ctx context.Context, submissionID string, cause error, terminal bool) error
}

type Sender interface {
	Send(ctx context.Context, submission Submission) error
}

type Config struct {
	BatchSize    int
	PollInterval time.Duration
	MaxAttempts  int
	Store        Store
	Sender       Sender
	ErrorHandler func(error)
}

type Dispatcher struct {
	batchSize    int
	pollInterval time.Duration
	maxAttempts  int
	store        Store
	sender       Sender
	errorHandler func(error)
}

func New(config Config) (*Dispatcher, error) {
	if config.BatchSize <= 0 ||
		config.PollInterval <= 0 ||
		config.MaxAttempts <= 0 ||
		config.Store == nil ||
		config.Sender == nil {
		return nil, ErrInvalidConfig
	}
	return &Dispatcher{
		batchSize:    config.BatchSize,
		pollInterval: config.PollInterval,
		maxAttempts:  config.MaxAttempts,
		store:        config.Store,
		sender:       config.Sender,
		errorHandler: config.ErrorHandler,
	}, nil
}

func (d *Dispatcher) RunOnce(ctx context.Context) error {
	submissions, err := d.store.Claim(ctx, d.batchSize)
	if err != nil {
		return err
	}
	for _, submission := range submissions {
		if err := d.sender.Send(ctx, copySubmission(submission)); err != nil {
			terminal := IsPermanent(err) || submission.Attempt >= d.maxAttempts
			if markErr := d.store.MarkFailed(ctx, submission.ID, err, terminal); markErr != nil {
				return markErr
			}
			continue
		}
		if err := d.store.MarkSubmitted(ctx, submission.ID); err != nil {
			return err
		}
	}
	return nil
}

func (d *Dispatcher) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := d.RunOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if d.errorHandler != nil {
				d.errorHandler(err)
			}
		}
		timer := time.NewTimer(d.pollInterval)
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

type permanentError struct {
	cause error
}

func (e permanentError) Error() string {
	return e.cause.Error()
}

func (e permanentError) Unwrap() error {
	return e.cause
}

func (e permanentError) Permanent() bool {
	return true
}

func Permanent(cause error) error {
	if cause == nil {
		return nil
	}
	return permanentError{cause: cause}
}

func IsPermanent(err error) bool {
	var classified interface {
		Permanent() bool
	}
	return errors.As(err, &classified) && classified.Permanent()
}

func copySubmission(submission Submission) Submission {
	submission.Payload = append([]byte(nil), submission.Payload...)
	return submission
}

func (s Submission) Validate() error {
	if s.ID == "" ||
		s.OrderID == "" ||
		s.ExternalStoreID == "" ||
		s.DestinationURL == "" ||
		len(s.Payload) == 0 ||
		s.Attempt <= 0 {
		return fmt.Errorf("%w: invalid claimed submission", ErrInvalidConfig)
	}
	return nil
}
