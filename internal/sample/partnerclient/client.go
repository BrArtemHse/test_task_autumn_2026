package partnerclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/contracts/partnercallbacks"
	"github.com/talense-tasks/backend-trainee-assignment-autumn-2026-flow-2-brartemhse-079cad3c/internal/contracts/partnerevents"
)

const defaultMaxResponseBytes int64 = 64 << 10

var (
	ErrInvalidConfig   = errors.New("invalid partner callback client config")
	ErrInvalidCallback = errors.New("invalid partner callback")
)

type Config struct {
	URL              string
	Token            string
	Timeout          time.Duration
	MaxResponseBytes int64
}

type Client struct {
	url              string
	token            string
	httpClient       *http.Client
	maxResponseBytes int64
}

type permanentError struct {
	err error
}

func (e permanentError) Error() string {
	return e.err.Error()
}

func (e permanentError) Unwrap() error {
	return e.err
}

func IsPermanent(err error) bool {
	var target permanentError
	return errors.As(err, &target)
}

func New(config Config) (*Client, error) {
	endpoint := strings.TrimSpace(config.URL)
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, fmt.Errorf("%w: absolute HTTP(S) url is required", ErrInvalidConfig)
	}
	token := strings.TrimSpace(config.Token)
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return nil, fmt.Errorf("%w: bearer token is required", ErrInvalidConfig)
	}
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
		url:   endpoint,
		token: token,
		httpClient: &http.Client{
			Timeout: config.Timeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		maxResponseBytes: maxResponseBytes,
	}, nil
}

func (c *Client) Send(ctx context.Context, statusCallback partnercallbacks.OrderStatusV1) error {
	if err := validateCallback(statusCallback); err != nil {
		return err
	}
	body, err := json.Marshal(statusCallback)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+c.token)
	request.Header.Set("X-Request-ID", statusCallback.CallbackID)

	response, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, c.maxResponseBytes))

	if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
		return nil
	}
	responseErr := fmt.Errorf("partner callback returned status %d", response.StatusCode)
	if response.StatusCode >= http.StatusMultipleChoices && response.StatusCode < http.StatusInternalServerError {
		return permanentError{err: responseErr}
	}
	return responseErr
}

func validateCallback(statusCallback partnercallbacks.OrderStatusV1) error {
	if statusCallback.CallbackID == "" ||
		statusCallback.OrderID == "" ||
		statusCallback.RestaurantID == "" ||
		statusCallback.ExternalStoreID == "" ||
		statusCallback.OccurredAt.IsZero() {
		return ErrInvalidCallback
	}
	if statusCallback.Status != partnerevents.AcceptedStatus && statusCallback.Status != partnerevents.RejectedStatus {
		return ErrInvalidCallback
	}
	return nil
}
