package modules

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ExtractYouTubeID extracts a video ID from common YouTube URL formats.
func ExtractYouTubeID(rawURL string) (string, bool) {
	// Examples:
	// https://www.youtube.com/watch?v=VIDEOID
	// https://youtu.be/VIDEOID
	// https://www.youtube.com/embed/VIDEOID
	if rawURL == "" {
		return "", false
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", false
	}
	// watch?v=
	if strings.Contains(u.Host, "youtube.com") {
		q := u.Query().Get("v")
		if q != "" {
			return q, true
		}
		// /embed/VIDEOID
		parts := strings.Split(strings.Trim(u.Path, "/"), "/")
		// Safe array access with bounds checking
		if len(parts) > 1 {
			for i := 0; i < len(parts)-1; i++ {
				if parts[i] == "embed" && i+1 < len(parts) {
					return parts[i+1], true
				}
			}
		}
	}
	// youtu.be/VIDEOID
	if strings.Contains(u.Host, "youtu.be") {
		id := strings.Trim(strings.Trim(u.Path, "/"), " ")
		if id != "" {
			return id, true
		}
	}
	return "", false
}

// parseCountString converts strings like "329,067,736 views", "1.2M", "45K" to an integer.
func parseCountString(s string) int {
	s = strings.ToUpper(strings.TrimSpace(s))
	// remove words like "VIEWS", "LIKES"
	s = strings.ReplaceAll(s, "VIEWS", "")
	s = strings.ReplaceAll(s, "LIKES", "")
	s = strings.TrimSpace(s)
	// handle K/M/B suffixes
	mult := 1.0
	if strings.HasSuffix(s, "K") {
		mult = 1_000
		s = strings.TrimSuffix(s, "K")
	} else if strings.HasSuffix(s, "M") {
		mult = 1_000_000
		s = strings.TrimSuffix(s, "M")
	} else if strings.HasSuffix(s, "B") {
		mult = 1_000_000_000
		s = strings.TrimSuffix(s, "B")
	}
	// remove commas and spaces
	s = strings.ReplaceAll(s, ",", "")
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return int(f * mult)
	}
	// fallback: extract digits
	// Safe regex compilation with panic recovery
	var digits []string
	func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("Warning: Regex compilation panic recovered: %v\n", r)
			}
		}()
		digits = regexp.MustCompile(`\d+`).FindAllString(s, -1)
	}()
	if len(digits) == 0 {
		return 0
	}
	joined := strings.Join(digits, "")
	if n, err := strconv.ParseInt(joined, 10, 64); err == nil {
		return int(n)
	}
	return 0
}

type scrapingDogVideo struct {
	ID            string `json:"id"`
	Title         string `json:"title"`
	LengthSeconds int    `json:"length_seconds"`
	Views         string `json:"views"`
	Likes         string `json:"likes"`
	Author        string `json:"author"`
	PublishedTime string `json:"published_time"`
	Description   string `json:"description"`
	Thumbnail     string `json:"thumbnail"`
}

type scrapingDogResponse struct {
	Video scrapingDogVideo `json:"video"`
}

// isRetryableError checks if the error suggests a retryable condition.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "context deadline exceeded") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "network is unreachable")
}

// isRetryableStatusCode checks if the HTTP status code suggests a retryable condition.
func isRetryableStatusCode(statusCode int) bool {
	// Retry on rate limits, server errors, and gateway timeouts
	return statusCode == 429 || // Rate limit
		statusCode == 502 || // Bad Gateway
		statusCode == 503 || // Service Unavailable
		statusCode == 504 // Gateway Timeout
}

// FetchYouTubeDetails calls ScrapingDog YouTube API with retry logic and returns the parsed response.
func FetchYouTubeDetails(ctx context.Context, videoID string) (*scrapingDogResponse, error) {
	apiKey := os.Getenv("Scrapping_Dog_API_Key")
	if apiKey == "" {
		return nil, fmt.Errorf("Scrapping_Dog_API_Key missing")
	}
	endpoint := "https://api.scrapingdog.com/youtube/video"
	q := url.Values{}
	q.Set("api_key", apiKey)
	q.Set("v", videoID)
	urlStr := endpoint + "?" + q.Encode()

	// Retry configuration: 3 attempts with exponential backoff
	maxRetries := 3
	baseDelay := 2 * time.Second // Increased base delay for rate limits

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
		if err != nil {
			return nil, err
		}

		client := &http.Client{Timeout: 15 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			// Retry on network/timeout errors
			if isRetryableError(err) && attempt < maxRetries-1 {
				delay := time.Duration(attempt+1) * baseDelay
				time.Sleep(delay)
				continue
			}
			return nil, err
		}

		// Handle HTTP status codes
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			// Safe response body close with panic recovery
			func() {
				defer func() {
					if r := recover(); r != nil {
						fmt.Printf("Warning: Panic recovered while closing response body: %v\n", r)
					}
				}()
				resp.Body.Close()
			}()
			lastErr = fmt.Errorf("scrapingdog status %d", resp.StatusCode)

			// Retry on retryable status codes
			if isRetryableStatusCode(resp.StatusCode) && attempt < maxRetries-1 {
				delay := time.Duration(attempt+1) * baseDelay
				// Add jitter to avoid thundering herd (random 0-1 second)
				jitter := time.Duration(attempt) * 500 * time.Millisecond
				time.Sleep(delay + jitter)
				continue
			}
			return nil, lastErr
		}

		// Success - parse response
		var out scrapingDogResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			// Safe response body close with panic recovery
			func() {
				defer func() {
					if r := recover(); r != nil {
						fmt.Printf("Warning: Panic recovered while closing response body: %v\n", r)
					}
				}()
				resp.Body.Close()
			}()
			return nil, err
		}
		// Safe response body close with panic recovery
		func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("Warning: Panic recovered while closing response body: %v\n", r)
				}
			}()
			resp.Body.Close()
		}()
		return &out, nil
	}

	return nil, lastErr
}

// MapScrapingDogToVideoFields maps the ScrapingDog response to our video fields.
type YouTubeEnrichment struct {
	VideoID         string
	Title           string
	ThumbnailURL    string
	ThumbnailWidth  int
	ThumbnailHeight int
	PublishDate     string
	Likes           int
	Views           int
	Description     string
}

func BuildEnrichment(sd *scrapingDogResponse) YouTubeEnrichment {
	if sd == nil {
		return YouTubeEnrichment{}
	}
	// Safe struct field access - structs can't be nil, but we can check for empty values
	v := sd.Video
	// Check if video data is empty (all fields are zero values)
	if v.ID == "" && v.Title == "" && v.Author == "" {
		// Return empty enrichment if video data appears to be empty
		return YouTubeEnrichment{}
	}
	return YouTubeEnrichment{
		VideoID:         v.ID,
		Title:           v.Title,
		ThumbnailURL:    v.Thumbnail,
		ThumbnailWidth:  0, // unknown from API; keep existing if present
		ThumbnailHeight: 0, // unknown from API; keep existing if present
		PublishDate:     v.PublishedTime,
		Likes:           parseCountString(v.Likes),
		Views:           parseCountString(v.Views),
		Description:     v.Description,
	}
}
