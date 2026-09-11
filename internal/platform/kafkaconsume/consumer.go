package kafkaconsume

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const defaultRetryDelay = time.Second

var ErrInvalidConfig = errors.New("invalid kafka consumer config")

type Message struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string][]byte
}

type Handler interface {
	Handle(ctx context.Context, message Message) error
}

type HandlerFunc func(ctx context.Context, message Message) error

func (f HandlerFunc) Handle(ctx context.Context, message Message) error {
	return f(ctx, message)
}

type Config struct {
	Brokers      []string
	Topic        string
	GroupID      string
	RetryDelay   time.Duration
	Handler      Handler
	ErrorHandler func(error)
}

type Record struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string][]byte
}

type reader interface {
	Fetch(ctx context.Context) (Record, error)
	Commit(ctx context.Context, record Record) error
	Close() error
}

type Consumer struct {
	reader       reader
	handler      Handler
	retryDelay   time.Duration
	errorHandler func(error)
}

func New(config Config) (*Consumer, error) {
	brokers := cleanValues(config.Brokers)
	topic := strings.TrimSpace(config.Topic)
	groupID := strings.TrimSpace(config.GroupID)
	if len(brokers) == 0 {
		return nil, fmt.Errorf("%w: at least one broker is required", ErrInvalidConfig)
	}
	if topic == "" {
		return nil, fmt.Errorf("%w: topic is required", ErrInvalidConfig)
	}
	if groupID == "" {
		return nil, fmt.Errorf("%w: group id is required", ErrInvalidConfig)
	}
	if config.Handler == nil {
		return nil, fmt.Errorf("%w: handler is required", ErrInvalidConfig)
	}

	retryDelay := config.RetryDelay
	if retryDelay == 0 {
		retryDelay = defaultRetryDelay
	}
	if retryDelay < 0 {
		return nil, fmt.Errorf("%w: retry delay must not be negative", ErrInvalidConfig)
	}

	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10 << 20,
		MaxWait:        500 * time.Millisecond,
		CommitInterval: 0,
	})
	return newConsumer(&readerAdapter{reader: kafkaReader}, config.Handler, retryDelay, config.ErrorHandler), nil
}

func newConsumer(source reader, handler Handler, retryDelay time.Duration, errorHandler func(error)) *Consumer {
	return &Consumer{
		reader:       source,
		handler:      handler,
		retryDelay:   retryDelay,
		errorHandler: errorHandler,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		record, err := c.reader.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.report(err)
			if err := waitRetry(ctx, c.retryDelay); err != nil {
				return err
			}
			continue
		}

		if err := c.handleUntilSuccess(ctx, record); err != nil {
			return err
		}
		if err := c.commitUntilSuccess(ctx, record); err != nil {
			return err
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

func (c *Consumer) handleUntilSuccess(ctx context.Context, record Record) error {
	message := record.message()
	for {
		if err := c.handler.Handle(ctx, message); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.report(err)
			if err := waitRetry(ctx, c.retryDelay); err != nil {
				return err
			}
			continue
		}
		return nil
	}
}

func (c *Consumer) commitUntilSuccess(ctx context.Context, record Record) error {
	for {
		if err := c.reader.Commit(ctx, record); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.report(err)
			if err := waitRetry(ctx, c.retryDelay); err != nil {
				return err
			}
			continue
		}
		return nil
	}
}

func (c *Consumer) report(err error) {
	if c.errorHandler != nil {
		c.errorHandler(err)
	}
}

func (r Record) message() Message {
	headers := make(map[string][]byte, len(r.Headers))
	for key, value := range r.Headers {
		headers[key] = append([]byte(nil), value...)
	}
	return Message{
		Topic:   r.Topic,
		Key:     append([]byte(nil), r.Key...),
		Value:   append([]byte(nil), r.Value...),
		Headers: headers,
	}
}

type readerAdapter struct {
	reader *kafka.Reader
}

func (r *readerAdapter) Fetch(ctx context.Context) (Record, error) {
	message, err := r.reader.FetchMessage(ctx)
	if err != nil {
		return Record{}, err
	}
	headers := make(map[string][]byte, len(message.Headers))
	for _, header := range message.Headers {
		headers[header.Key] = append([]byte(nil), header.Value...)
	}
	return Record{
		Topic:     message.Topic,
		Partition: message.Partition,
		Offset:    message.Offset,
		Key:       append([]byte(nil), message.Key...),
		Value:     append([]byte(nil), message.Value...),
		Headers:   headers,
	}, nil
}

func (r *readerAdapter) Commit(ctx context.Context, record Record) error {
	return r.reader.CommitMessages(ctx, kafka.Message{
		Topic:     record.Topic,
		Partition: record.Partition,
		Offset:    record.Offset,
	})
}

func (r *readerAdapter) Close() error {
	return r.reader.Close()
}

func waitRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func cleanValues(values []string) []string {
	cleaned := make([]string, 0, len(values))
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			cleaned = append(cleaned, value)
		}
	}
	return cleaned
}
