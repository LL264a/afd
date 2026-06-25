package downloader

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.uber.org/zap"
)

type RetryConfig struct {
	MaxRetries      int
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:      3,
		InitialInterval: 1 * time.Second,
		MaxInterval:     60 * time.Second,
		Multiplier:      2.0,
	}
}

type RetryableError struct {
	err       error
	retryable bool
}

func (e *RetryableError) Error() string {
	return e.err.Error()
}

func (e *RetryableError) Unwrap() error {
	return e.err
}

func (e *RetryableError) IsRetryable() bool {
	return e.retryable
}

func NewRetryableError(err error, retryable bool) *RetryableError {
	return &RetryableError{
		err:       err,
		retryable: retryable,
	}
}

func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	var retryableErr *RetryableError
	if errors.As(err, &retryableErr) {
		return retryableErr.IsRetryable()
	}

	errStr := err.Error()
	errLower := strings.ToLower(errStr)

	if strings.Contains(errLower, "timeout") ||
		strings.Contains(errLower, "i/o timeout") ||
		strings.Contains(errLower, "context deadline exceeded") {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return true
		}
		if _, ok := netErr.(*net.DNSError); ok {
			return true
		}
		if _, ok := netErr.(*net.AddrError); ok {
			if strings.Contains(errLower, "connection refused") {
				return true
			}
		}
	}

	urlErr, ok := err.(*url.Error)
	if ok {
		return IsRetryableError(urlErr.Err)
	}

	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		statusCode := httpErr.StatusCode
		if statusCode >= 500 {
			return true
		}
		if statusCode == http.StatusTooManyRequests {
			return true
		}
		return false
	}

	return false
}

func IsPermanentError(err error) bool {
	if err == nil {
		return false
	}

	if !IsRetryableError(err) {
		return true
	}

	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		statusCode := httpErr.StatusCode
		if statusCode >= 400 && statusCode < 500 && statusCode != http.StatusTooManyRequests {
			return true
		}
	}

	return false
}

type HTTPError struct {
	StatusCode int
	Message    string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("http error: %d - %s", e.StatusCode, e.Message)
}

func NewHTTPError(statusCode int, message string) *HTTPError {
	return &HTTPError{
		StatusCode: statusCode,
		Message:    message,
	}
}

func IsHTTPError(err error) bool {
	var httpErr *HTTPError
	return errors.As(err, &httpErr)
}

func GetHTTPStatusCode(err error) int {
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode
	}
	return 0
}

func DoWithRetry(ctx context.Context, config RetryConfig, fn func() error) error {
	if config.MaxRetries <= 0 {
		return fn()
	}

	var lastErr error
	interval := config.InitialInterval

	// 使用 NewTimer 替代 time.After，避免重试循环中累积未触发的定时器。
	backoffTimer := time.NewTimer(0)
	defer backoffTimer.Stop()
	if !backoffTimer.Stop() {
		<-backoffTimer.C
	}

	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		if IsPermanentError(lastErr) {
			return lastErr
		}

		if attempt == config.MaxRetries {
			break
		}

		backoff := time.Duration(float64(interval) * config.Multiplier)
		if backoff > config.MaxInterval {
			backoff = config.MaxInterval
		}

		backoffTimer.Reset(backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-backoffTimer.C:
		}

		interval = backoff
	}

	return fmt.Errorf("max retries (%d) exceeded: %w", config.MaxRetries, lastErr)
}

func DoWithRetryWithLogger(ctx context.Context, config RetryConfig, logger *zap.SugaredLogger, fn func() error) error {
	if config.MaxRetries <= 0 {
		return fn()
	}

	var lastErr error
	interval := config.InitialInterval

	// 使用 NewTimer 替代 time.After，避免重试循环中累积未触发的定时器。
	backoffTimer := time.NewTimer(0)
	defer backoffTimer.Stop()
	if !backoffTimer.Stop() {
		<-backoffTimer.C
	}

	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		lastErr = fn()
		if lastErr == nil {
			if attempt > 0 {
				logger.Infow("retry succeeded", "attempts", attempt+1)
			}
			return nil
		}

		if IsPermanentError(lastErr) {
			logger.Debugw("permanent error, not retrying", "error", lastErr)
			return lastErr
		}

		if attempt == config.MaxRetries {
			break
		}

		backoff := time.Duration(float64(interval) * config.Multiplier)
		if backoff > config.MaxInterval {
			backoff = config.MaxInterval
		}

		logger.Warnw("retrying after error",
			"attempt", attempt+1,
			"max_retries", config.MaxRetries,
			"backoff", backoff,
			"error", lastErr,
		)

		backoffTimer.Reset(backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-backoffTimer.C:
		}

		interval = backoff
	}

	return fmt.Errorf("max retries (%d) exceeded: %w", config.MaxRetries, lastErr)
}
