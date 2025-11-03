package modules

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	importModel "github.com/InvicttusGIT/BlinkAi_Merged/internal/models"
)

// isRetryableNetworkError checks if the error suggests a retryable network condition.
func isRetryableNetworkError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection was forcibly closed") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "context deadline exceeded") ||
		strings.Contains(msg, "network is unreachable") ||
		strings.Contains(msg, "connection refused")
}

// callExtractorAPI makes HTTP POST request to the extractor API
func callExtractorAPI(apiURL string, requestBody map[string]interface{}) error {
	// Get API key from environment
	apiKey := os.Getenv("Youtube_Video_playable_API")
	if apiKey == "" {
		return fmt.Errorf("Youtube_Video_playable_API environment variable not set")
	}
	fmt.Print("sending batch request to the ytdl extractor")

	// Marshal request body to JSON
	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request body: %v", err)
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", apiURL, strings.NewReader(string(jsonBody)))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", apiKey)

	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 10 * time.Minute, // 10 minutes timeout for video processing
	}

	// Make the request with retry logic for network issues
	var resp *http.Response
	var requestErr error
	maxRetries := 3
	baseDelay := 2 * time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		resp, requestErr = client.Do(req)
		if requestErr == nil {
			break
		}

		// Check if error is retryable (network issues)
		if isRetryableNetworkError(requestErr) && attempt < maxRetries-1 {
			delay := time.Duration(attempt+1) * baseDelay
			fmt.Printf("Retrying extractor API call (attempt %d/%d) after %v: %v\n", attempt+1, maxRetries, delay, requestErr)
			time.Sleep(delay)
			continue
		}

		return fmt.Errorf("failed to make request after %d attempts: %v", maxRetries, requestErr)
	}
	defer resp.Body.Close()

	// For streaming/SSE style responses, we parse line-by-line while also
	// tolerating non-streamed JSON bodies. We'll accumulate summary stats
	// and failed URLs so they are clearly visible at the end.

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	scanner := bufio.NewScanner(resp.Body)
	// Increase the scanner buffer to handle large JSON lines
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	// Running tallies (best-effort based on streamed objects)
	var finalSearchID string
	var finalMessage string
	var totalProcessed, totalSuccessful, totalFailed int
	failedURLs := make([]string, 0)

	decodeAndHandle := func(line string) {
		// Some servers use SSE: ignore non-JSON control lines like "event: ..."
		if strings.HasPrefix(line, "event:") {
			return
		}
		// SSE payload lines start with "data:" — strip the prefix if present
		if strings.HasPrefix(line, "data:") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
		if line == "" {
			return
		}
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			// Ignore non-JSON lines silently to avoid noisy logs containing long URLs
			return
		}

		// Capture top-level summary when present
		if v, ok := m["search_id"].(string); ok && v != "" {
			finalSearchID = v
		}
		if v, ok := m["message"].(string); ok {
			finalMessage = v
		}
		if v, ok := m["total_processed"].(float64); ok {
			totalProcessed = int(v)
		}
		if v, ok := m["successful"].(float64); ok {
			totalSuccessful = int(v)
		}
		if v, ok := m["failed"].(float64); ok {
			totalFailed = int(v)
		}

		// Also support nested progress object
		if prog, ok := m["progress"].(map[string]interface{}); ok {
			if v, ok := prog["successful"].(float64); ok {
				totalSuccessful = int(v)
			}
			if v, ok := prog["failed"].(float64); ok {
				totalFailed = int(v)
			}
			if v, ok := prog["total"].(float64); ok {
				totalProcessed = int(v)
			}
		}

		// Track failed URLs only if the event hints at failure
		// We consider an event a failure if it has a url and either an error field,
		// or the status/message indicates failure.
		urlStr, _ := m["url"].(string)
		hasError := false
		if _, ok := m["error"]; ok {
			hasError = true
		}
		if status, ok := m["status"].(string); ok && strings.Contains(strings.ToLower(status), "fail") {
			hasError = true
		}
		if msg, ok := m["message"].(string); ok && strings.Contains(strings.ToLower(msg), "fail") {
			hasError = true
		}
		// If it's a per-video success payload it typically contains a nested "data" object.
		if urlStr != "" && hasError {
			failedURLs = append(failedURLs, urlStr)
			// Print the failed URL immediately for visibility
			fmt.Printf("extractor_failed_url: %s\n", urlStr)
		}
	}

	// If the API streams, consume line by line; if not, the scanner will likely
	// read a single big JSON. We handle both gracefully.
	hadLines := false
	for scanner.Scan() {
		hadLines = true
		decodeAndHandle(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		// Check if it's a network error that should be retried
		if isRetryableNetworkError(err) {
			return fmt.Errorf("network error during response scanning (retryable): %v", err)
		}
		// Fallback: try to read the remaining body for diagnostics
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to scan extractor response: %v; tail=%s", err, string(body))
	}

	if !hadLines {
		// Non-streaming JSON; read and process once
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response body: %v", err)
		}
		decodeAndHandle(string(body))
	}

	// Final summary for easy visibility in logs
	if finalMessage != "" || totalProcessed > 0 {
		fmt.Printf("ytdl_extractor_complete: {\"search_id\":\"%s\", \"message\":\"%s\", \"total_processed\":%d, \"successful\":%d, \"failed\":%d}\n", finalSearchID, finalMessage, totalProcessed, totalSuccessful, totalFailed)
	}
	if len(failedURLs) > 0 {
		fmt.Println("extractor_failed_urls:")
		for _, u := range failedURLs {
			fmt.Printf("- %s\n", u)
		}
	}
	return nil
}

// ProcessVideoExtraction extracts video URLs from combined items and sends them to the appropriate extractor API
func ProcessVideoExtraction(combinedItems []importModel.CombinedItem, searchRecord *importModel.Search, platform string) {
	// Extract video URLs from combined_global
	var videoURLs []string
	for _, item := range combinedItems {
		if item.Kind == "video" {
			if videoItem, ok := item.Payload.(importModel.VideoItem); ok {
				videoURLs = append(videoURLs, videoItem.URL)
			}
		}
	}

	// Only proceed if we have video URLs
	if len(videoURLs) == 0 {
		fmt.Println("No video URLs found to send to extractor API")
		return
	}

	// Prepare request body
	requestBody := map[string]interface{}{
		"urls":          videoURLs,
		"search_id":     searchRecord.SearchID,
		"use_proxy":     true,
		"video_height":  1080,
		"audio_bitrate": 160,
	}

	// Choose API endpoint based on platform
	var apiURL string
	if platform == "roku" {
		apiURL = "https://roku-tube-dev2118.replit.app/api/bulk-roku-extractor"
	} else {
		apiURL = "https://roku-tube-dev2118.replit.app/api/bulk-extractor"
	}

	// Make API call
	if err := callExtractorAPI(apiURL, requestBody); err != nil {
		fmt.Printf("Failed to call extractor API: %v\n", err)
	} else {
		fmt.Printf("Successfully sent %d video URLs to %s ytdl extractor API\n", len(videoURLs), platform)
	}
}
