package outbox_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/outbox"
)

func TestRelayRunOncePublishesClaimedMessagesAndMarksPublished(t *testing.T) {
	store := &fakeStore{
		claimed: []outbox.Message{
			{
				EventID: "evt-1",
				Topic:   "orders.events.v1",
				Key:     "ord-1",
				Value:   []byte(`{"order_id":"ord-1"}`),
				Headers: map[string]string{
					"event_type":    "order.created.v1",
					"event_version": "1",
				},
			},
		},
	}
	publisher := &fakePublisher{}
	relay, err := outbox.NewRelay(outbox.Config{
		BatchSize:    10,
		PollInterval: time.Millisecond,
		Store:        store,
		Publisher:    publisher,
	})
	if err != nil {
		t.Fatalf("NewRelay() error = %v", err)
	}

	if err := relay.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}

	if store.claimLimit != 10 {
		t.Fatalf("claim limit = %d, want 10", store.claimLimit)
	}
	if len(publisher.published) != 1 {
		t.Fatalf("published messages = %d, want 1", len(publisher.published))
	}
	published := publisher.published[0]
	if published.EventID != "evt-1" || published.Topic != "orders.events.v1" || published.Key != "ord-1" {
		t.Fatalf("published identity = %#v", published)
	}
	if string(published.Value) != `{"order_id":"ord-1"}` {
		t.Fatalf("published payload = %s", published.Value)
	}
	if !reflect.DeepEqual(published.Headers, map[string]string{"event_type": "order.created.v1", "event_version": "1"}) {
		t.Fatalf("published headers = %#v", published.Headers)
	}
	if !reflect.DeepEqual(store.published, []string{"evt-1"}) {
		t.Fatalf("marked published = %#v, want [evt-1]", store.published)
	}
	if len(store.failed) != 0 {
		t.Fatalf("failed marks = %#v, want none", store.failed)
	}
}

func TestRelayRunOnceMarksMessageFailedWhenPublishFails(t *testing.T) {
	publishErr := errors.New("broker unavailable")
	store := &fakeStore{
		claimed: []outbox.Message{
			{EventID: "evt-1", Topic: "orders.events.v1", Key: "ord-1", Value: []byte(`{}`)},
			{EventID: "evt-2", Topic: "orders.events.v1", Key: "ord-2", Value: []byte(`{}`)},
		},
	}
	publisher := &fakePublisher{failures: map[string]error{"evt-1": publishErr}}
	relay, err := outbox.NewRelay(outbox.Config{
		BatchSize:    2,
		PollInterval: time.Millisecond,
		Store:        store,
		Publisher:    publisher,
	})
	if err != nil {
		t.Fatalf("NewRelay() error = %v", err)
	}

	if err := relay.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}

	if !reflect.DeepEqual(store.published, []string{"evt-2"}) {
		t.Fatalf("published marks = %#v, want [evt-2]", store.published)
	}
	if len(store.failed) != 1 {
		t.Fatalf("failed marks = %d, want 1", len(store.failed))
	}
	if store.failed[0].eventID != "evt-1" {
		t.Fatalf("failed event id = %q, want evt-1", store.failed[0].eventID)
	}
	if !errors.Is(store.failed[0].cause, publishErr) {
		t.Fatalf("failed cause = %v, want %v", store.failed[0].cause, publishErr)
	}
}

func TestRelayRunOnceRejectsInvalidConfig(t *testing.T) {
	_, err := outbox.NewRelay(outbox.Config{
		BatchSize:    0,
		PollInterval: time.Millisecond,
		Store:        &fakeStore{},
		Publisher:    &fakePublisher{},
	})
	if !errors.Is(err, outbox.ErrInvalidConfig) {
		t.Fatalf("NewRelay() error = %v, want %v", err, outbox.ErrInvalidConfig)
	}

	_, err = outbox.NewRelay(outbox.Config{
		BatchSize:    1,
		PollInterval: time.Millisecond,
		Publisher:    &fakePublisher{},
	})
	if !errors.Is(err, outbox.ErrInvalidConfig) {
		t.Fatalf("NewRelay() missing store error = %v, want %v", err, outbox.ErrInvalidConfig)
	}

	_, err = outbox.NewRelay(outbox.Config{
		BatchSize:    1,
		PollInterval: time.Millisecond,
		Store:        &fakeStore{},
	})
	if !errors.Is(err, outbox.ErrInvalidConfig) {
		t.Fatalf("NewRelay() missing publisher error = %v, want %v", err, outbox.ErrInvalidConfig)
	}
}

func TestRelayRunStopsWhenContextIsCanceled(t *testing.T) {
	store := &fakeStore{}
	relay, err := outbox.NewRelay(outbox.Config{
		BatchSize:    1,
		PollInterval: time.Millisecond,
		Store:        store,
		Publisher:    &fakePublisher{},
	})
	if err != nil {
		t.Fatalf("NewRelay() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := relay.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context canceled", err)
	}
	if store.claims != 0 {
		t.Fatalf("claim calls = %d, want 0 after pre-canceled context", store.claims)
	}
}

func TestRelayRunReportsRunOnceErrorsAndContinuesUntilCanceled(t *testing.T) {
	claimErr := errors.New("database timeout")
	store := &fakeStore{claimErrors: []error{claimErr}}
	var reported []error
	ctx, cancel := context.WithCancel(context.Background())
	relay, err := outbox.NewRelay(outbox.Config{
		BatchSize:    1,
		PollInterval: time.Millisecond,
		Store:        store,
		Publisher:    &fakePublisher{},
		ErrorHandler: func(err error) {
			reported = append(reported, err)
			cancel()
		},
	})
	if err != nil {
		t.Fatalf("NewRelay() error = %v", err)
	}

	if err := relay.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context canceled", err)
	}
	if len(reported) != 1 {
		t.Fatalf("reported errors = %d, want 1", len(reported))
	}
	if !errors.Is(reported[0], claimErr) {
		t.Fatalf("reported error = %v, want %v", reported[0], claimErr)
	}
}

type fakeStore struct {
	claimed     []outbox.Message
	claimErrors []error
	claimLimit  int
	claims      int
	published   []string
	failed      []failedMark
}

type failedMark struct {
	eventID string
	cause   error
}

func (s *fakeStore) Claim(_ context.Context, limit int) ([]outbox.Message, error) {
	s.claims++
	s.claimLimit = limit
	if len(s.claimErrors) > 0 {
		err := s.claimErrors[0]
		s.claimErrors = s.claimErrors[1:]
		return nil, err
	}
	return append([]outbox.Message(nil), s.claimed...), nil
}

func (s *fakeStore) MarkPublished(_ context.Context, eventID string) error {
	s.published = append(s.published, eventID)
	return nil
}

func (s *fakeStore) MarkFailed(_ context.Context, eventID string, cause error) error {
	s.failed = append(s.failed, failedMark{eventID: eventID, cause: cause})
	return nil
}

type fakePublisher struct {
	published []outbox.Message
	failures  map[string]error
}

func (p *fakePublisher) Publish(_ context.Context, message outbox.Message) error {
	p.published = append(p.published, message)
	if err := p.failures[message.EventID]; err != nil {
		return err
	}
	return nil
}
