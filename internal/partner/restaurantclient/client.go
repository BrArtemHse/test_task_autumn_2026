package restaurantclient

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/partner/dispatch"
)

const defaultMaxResponseBytes int64 = 64 << 10

var ErrInvalidConfig = errors.New("invalid restaurant client config")

type Config struct {
	Timeout          time.Duration
	MaxResponseBytes int64
}

type Client struct {
	httpClient       *http.Client
	maxResponseBytes int64
}

func New(config Config) (*Client, error) {
	if config.Timeout <= 0 {
		return nil, fmt.Errorf("%w: timeout must be positive", ErrInvalidConfig)
	}
	maxResponseBytes := config.MaxResponseBytes
	if maxResponseBytes == 0 {
		maxResponseBytes = defaultMaxResponseBytes
	}
	if maxResponseBytes < 0 {
		return nil, fmt.Errorf("%w: max response bytes must not be negative", ErrInvalidConfig)
	}
	return &Client{
		httpClient: &http.Client{
			Timeout: config.Timeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		maxResponseBytes: maxResponseBytes,
	}, nil
}

func (c *Client) Send(ctx context.Context, submission dispatch.Submission) error {
	if err := submission.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	destination, err := url.Parse(submission.DestinationURL)
	if err != nil || destination.Host == "" || (destination.Scheme != "http" && destination.Scheme != "https") {
		return dispatch.Permanent(fmt.Errorf("invalid restaurant destination URL"))
	}

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		destination.String(),
		bytes.NewReader(submission.Payload),
	)
	if err != nil {
		return dispatch.Permanent(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", submission.OrderID)
	request.Header.Set("X-External-Store-ID", submission.ExternalStoreID)
	request.Header.Set("X-Submission-ID", submission.ID)

	response, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, c.maxResponseBytes))

	if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
		return nil
	}
	statusErr := fmt.Errorf("restaurant returned HTTP %d", response.StatusCode)
	if response.StatusCode >= http.StatusBadRequest && response.StatusCode < http.StatusInternalServerError {
		return dispatch.Permanent(statusErr)
	}
	return statusErr
}
