package kafkapub

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/platform/outbox"
)

var ErrInvalidConfig = errors.New("invalid kafka publisher config")

type Publisher struct {
	writer *kafka.Writer
}

func NewPublisher(brokers []string) (*Publisher, error) {
	cleaned := cleanBrokers(brokers)
	if len(cleaned) == 0 {
		return nil, fmt.Errorf("%w: at least one broker is required", ErrInvalidConfig)
	}

	return &Publisher{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(cleaned...),
			Balancer:               &kafka.Hash{},
			BatchSize:              1,
			BatchTimeout:           10 * time.Millisecond,
			RequiredAcks:           kafka.RequireAll,
			Async:                  false,
			AllowAutoTopicCreation: true,
		},
	}, nil
}

func (p *Publisher) Publish(ctx context.Context, message outbox.Message) error {
	return p.writer.WriteMessages(ctx, buildKafkaMessage(message))
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}

func buildKafkaMessage(message outbox.Message) kafka.Message {
	headers := make(map[string]string, len(message.Headers)+1)
	for key, value := range message.Headers {
		headers[key] = value
	}
	headers["event_id"] = message.EventID

	keys := make([]string, 0, len(headers))
	for key := range headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	kafkaHeaders := make([]kafka.Header, 0, len(keys))
	for _, key := range keys {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{
			Key:   key,
			Value: []byte(headers[key]),
		})
	}

	return kafka.Message{
		Topic:   message.Topic,
		Key:     append([]byte(nil), message.Key...),
		Value:   append([]byte(nil), message.Value...),
		Headers: kafkaHeaders,
	}
}

func cleanBrokers(brokers []string) []string {
	cleaned := make([]string, 0, len(brokers))
	for _, broker := range brokers {
		value := strings.TrimSpace(broker)
		if value != "" {
			cleaned = append(cleaned, value)
		}
	}
	return cleaned
}
