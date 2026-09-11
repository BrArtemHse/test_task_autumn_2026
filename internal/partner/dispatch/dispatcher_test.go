package dispatch_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/dispatch"
)

func TestDispatcherRunOnceSendsAndMarksSubmitted(t *testing.T) {
	submission := dispatch.Submission{ID: "sub-1", OrderID: "ord-1", Attempt: 1}
	store := &storeStub{claimed: []dispatch.Submission{submission}}
	sender := &senderStub{}
	dispatcher, err := dispatch.New(dispatch.Config{
		BatchSize:    10,
		PollInterval: time.Millisecond,
		MaxAttempts:  3,
		Store:        store,
		Sender:       sender,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := dispatcher.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if !reflect.DeepEqual(sender.sent, []dispatch.Submission{submission}) {
		t.Fatalf("sent = %#v", sender.sent)
	}
	if !reflect.DeepEqual(store.submitted, []string{"sub-1"}) {
		t.Fatalf("submitted = %#v", store.submitted)
	}
}

func TestDispatcherRunOnceSchedulesTransientFailure(t *testing.T) {
	sendErr := errors.New("connection reset")
	store := &storeStub{claimed: []dispatch.Submission{{ID: "sub-1", OrderID: "ord-1", Attempt: 1}}}
	dispatcher, err := dispatch.New(dispatch.Config{
		BatchSize: 1, PollInterval: time.Millisecond, MaxAttempts: 3,
		Store: store, Sender: &senderStub{err: sendErr},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := dispatcher.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if len(store.failures) != 1 || store.failures[0].terminal {
		t.Fatalf("failures = %#v, want transient", store.failures)
	}
}

func TestDispatcherRunOnceMarksPermanentOrExhaustedFailureTerminal(t *testing.T) {
	tests := []struct {
		name        string
		attempt     int
		sendErr     error
		maxAttempts int
	}{
		{name: "permanent", attempt: 1, sendErr: dispatch.Permanent(errors.New("bad request")), maxAttempts: 3},
		{name: "exhausted", attempt: 3, sendErr: errors.New("timeout"), maxAttempts: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &storeStub{claimed: []dispatch.Submission{{ID: "sub-1", Attempt: tt.attempt}}}
			dispatcher, err := dispatch.New(dispatch.Config{
				BatchSize: 1, PollInterval: time.Millisecond, MaxAttempts: tt.maxAttempts,
				Store: store, Sender: &senderStub{err: tt.sendErr},
			})
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			if err := dispatcher.RunOnce(context.Background()); err != nil {
				t.Fatalf("RunOnce() error = %v", err)
			}
			if len(store.failures) != 1 || !store.failures[0].terminal {
				t.Fatalf("failures = %#v, want terminal", store.failures)
			}
		})
	}
}

type storeStub struct {
	claimed   []dispatch.Submission
	submitted []string
	failures  []failure
}

type failure struct {
	id       string
	cause    error
	terminal bool
}

func (s *storeStub) Claim(_ context.Context, _ int) ([]dispatch.Submission, error) {
	return append([]dispatch.Submission(nil), s.claimed...), nil
}

func (s *storeStub) MarkSubmitted(_ context.Context, id string) error {
	s.submitted = append(s.submitted, id)
	return nil
}

func (s *storeStub) MarkFailed(_ context.Context, id string, cause error, terminal bool) error {
	s.failures = append(s.failures, failure{id: id, cause: cause, terminal: terminal})
	return nil
}

type senderStub struct {
	sent []dispatch.Submission
	err  error
}

func (s *senderStub) Send(_ context.Context, submission dispatch.Submission) error {
	s.sent = append(s.sent, submission)
	return s.err
}
