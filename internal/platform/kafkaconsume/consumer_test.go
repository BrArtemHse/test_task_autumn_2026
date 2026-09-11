package kafkaconsume

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestConsumerRetriesSameRecordBeforeCommit(t *testing.T) {
	record := Record{Topic: "orders.events.v1", Partition: 1, Offset: 7, Key: []byte("ord-1"), Value: []byte("{}")}
	reader := &fakeReader{records: []Record{record}}
	handler := &fakeHandler{failures: []error{errors.New("database unavailable")}}
	ctx, cancel := context.WithCancel(context.Background())
	consumer := newConsumer(reader, handler, time.Millisecond, nil)
	reader.onCommit = cancel

	err := consumer.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context canceled", err)
	}
	if reader.fetches != 1 {
		t.Fatalf("fetch calls = %d, want 1", reader.fetches)
	}
	if handler.calls != 2 {
		t.Fatalf("handler calls = %d, want 2", handler.calls)
	}
	if reader.commits != 1 {
		t.Fatalf("commit calls = %d, want 1", reader.commits)
	}
	if reader.committed.Offset != 7 {
		t.Fatalf("committed offset = %d, want 7", reader.committed.Offset)
	}
}

func TestConsumerRetriesCommitWithoutRepeatingHandledEffect(t *testing.T) {
	record := Record{Topic: "orders.events.v1", Partition: 0, Offset: 3, Key: []byte("ord-1"), Value: []byte("{}")}
	reader := &fakeReader{
		records:        []Record{record},
		commitFailures: []error{errors.New("coordinator unavailable")},
	}
	handler := &fakeHandler{}
	ctx, cancel := context.WithCancel(context.Background())
	consumer := newConsumer(reader, handler, time.Millisecond, nil)
	reader.onCommitSuccess = cancel

	err := consumer.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context canceled", err)
	}
	if handler.calls != 1 {
		t.Fatalf("handler calls = %d, want 1", handler.calls)
	}
	if reader.commits != 2 {
		t.Fatalf("commit calls = %d, want 2", reader.commits)
	}
}

func TestNewConsumerRejectsInvalidConfig(t *testing.T) {
	_, err := New(Config{Topic: "orders.events.v1", GroupID: "partner-service", Handler: &fakeHandler{}})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New() error = %v, want %v", err, ErrInvalidConfig)
	}
}

type fakeReader struct {
	records         []Record
	fetches         int
	commits         int
	committed       Record
	commitFailures  []error
	onCommit        context.CancelFunc
	onCommitSuccess context.CancelFunc
}

func (r *fakeReader) Fetch(ctx context.Context) (Record, error) {
	r.fetches++
	if len(r.records) == 0 {
		<-ctx.Done()
		return Record{}, ctx.Err()
	}
	record := r.records[0]
	r.records = r.records[1:]
	return record, nil
}

func (r *fakeReader) Commit(_ context.Context, record Record) error {
	r.commits++
	r.committed = record
	if len(r.commitFailures) > 0 {
		err := r.commitFailures[0]
		r.commitFailures = r.commitFailures[1:]
		return err
	}
	if r.onCommit != nil {
		r.onCommit()
	}
	if r.onCommitSuccess != nil {
		r.onCommitSuccess()
	}
	return nil
}

func (r *fakeReader) Close() error {
	return nil
}

type fakeHandler struct {
	calls    int
	failures []error
}

func (h *fakeHandler) Handle(_ context.Context, _ Message) error {
	h.calls++
	if len(h.failures) == 0 {
		return nil
	}
	err := h.failures[0]
	h.failures = h.failures[1:]
	return err
}
