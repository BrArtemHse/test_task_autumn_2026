package events

import (
	"errors"
	"time"
)

var ErrInvalidEnvelope = errors.New("invalid event envelope")

type NewEnvelopeInput struct {
	EventID       string
	EventType     string
	EventVersion  int
	OccurredAt    time.Time
	Producer      string
	AggregateType string
	AggregateID   string
	PartitionKey  string
	CorrelationID string
	CausationID   string
	Payload       []byte
}

type Envelope struct {
	EventID       string
	EventType     string
	EventVersion  int
	OccurredAt    time.Time
	Producer      string
	AggregateType string
	AggregateID   string
	PartitionKey  string
	CorrelationID string
	CausationID   string
	Payload       []byte
}

func NewEnvelope(input NewEnvelopeInput) (Envelope, error) {
	if input.EventID == "" ||
		input.EventType == "" ||
		input.EventVersion <= 0 ||
		input.Producer == "" ||
		input.AggregateType == "" ||
		input.AggregateID == "" ||
		input.PartitionKey == "" ||
		len(input.Payload) == 0 {
		return Envelope{}, ErrInvalidEnvelope
	}

	occurredAt := input.OccurredAt
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}

	return Envelope{
		EventID:       input.EventID,
		EventType:     input.EventType,
		EventVersion:  input.EventVersion,
		OccurredAt:    occurredAt,
		Producer:      input.Producer,
		AggregateType: input.AggregateType,
		AggregateID:   input.AggregateID,
		PartitionKey:  input.PartitionKey,
		CorrelationID: input.CorrelationID,
		CausationID:   input.CausationID,
		Payload:       append([]byte(nil), input.Payload...),
	}, nil
}
