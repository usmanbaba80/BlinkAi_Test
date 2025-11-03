package services

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// urlQueryEscape safely escapes query values
func urlQueryEscape(s string) string {
	return url.QueryEscape(strings.ReplaceAll(s, "\n", ""))
}

// RetryConfig holds retry configuration
type RetryConfig struct {
	MaxRetries   int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Timeout      time.Duration
}

// DefaultRetryConfig returns default retry settings
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:   2,
		InitialDelay: 100 * time.Millisecond,
		MaxDelay:     3 * time.Second,
		Timeout:      30 * time.Second,  // Increased from 7s to allow time for reading large responses
	}
}

// retryHTTPGet performs HTTP GET with retry logic for 5xx and 429 errors
func retryHTTPGet(targetURL string, config RetryConfig) (*http.Response, error) {
	delay := config.InitialDelay
	var lastErr error

	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		if attempt > 0 {
			log.Printf("Retry attempt %d/%d after %v delay", attempt, config.MaxRetries, delay)
			time.Sleep(delay)

			// Exponential backoff
			delay *= 2
			if delay > config.MaxDelay {
				delay = config.MaxDelay
			}
		}

		// Create context with timeout (long enough for full response)
		ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
		req, err := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
		if err != nil {
			cancel()
			return nil, err
		}

		resp, err := http.DefaultClient.Do(req)

		// Network/connection error - retry
		if err != nil {
			cancel()
			lastErr = err
			log.Printf("Attempt %d/%d failed with error: %v", attempt+1, config.MaxRetries+1, err)
			if attempt < config.MaxRetries {
				continue
			}
			return nil, fmt.Errorf("all retry attempts exhausted: %w", err)
		}

		// Check for retryable status codes (5xx or 429)
		if resp.StatusCode >= 500 || resp.StatusCode == 429 {
			resp.Body.Close()
			cancel()
			lastErr = fmt.Errorf("received status code %d", resp.StatusCode)
			log.Printf("Attempt %d/%d failed with status %d - retrying", attempt+1, config.MaxRetries+1, resp.StatusCode)
			if attempt < config.MaxRetries {
				continue
			}
			return nil, fmt.Errorf("all retry attempts exhausted: %w", lastErr)
		}

		// Success (2xx, 3xx, 4xx except 429)
		log.Printf("Request succeeded with status %d", resp.StatusCode)
		
		// Return response with context still active for body reading
		// Caller should defer Close() on resp.Body
		// Note: context will expire after config.Timeout even if body not read
		return resp, nil
	}

	return nil, lastErr
}
