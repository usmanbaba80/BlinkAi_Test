package modules

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/InvicttusGIT/BlinkAi_SearchEngineApps_Productivity_Backend/models"
	"github.com/PuerkitoBio/goquery"
)

var httpClient = &http.Client{
	Timeout: 15 * time.Second,
	Transport: &http.Transport{
		ForceAttemptHTTP2:      false,       // Force HTTP/1.1
		MaxResponseHeaderBytes: 1024 * 1024, // 1MB limit for headers
		MaxIdleConns:           100,
		MaxIdleConnsPerHost:    10,
		IdleConnTimeout:        90 * time.Second,
	},
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 5 {
			return http.ErrUseLastResponse
		}
		return nil
	},
}

// getTimeoutsForURL returns a progressive timeout series for the given URL.
// It is designed to be extended for per-URL/per-content customizations.
func getTimeoutsForURL(_ string) []time.Duration {
	// Progressive timeouts: 8s -> 15s -> 30s
	return []time.Duration{8 * time.Second, 15 * time.Second, 30 * time.Second}
}

// isRetryableTimeout checks if the error suggests a timeout or context deadline exceeded.
func isRetryableTimeout(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "timeout") || strings.Contains(msg, "context deadline exceeded")
}

// isRetryableParsingError checks if the error suggests a retryable parsing failure.
func isRetryableParsingError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "context deadline exceeded") ||
		strings.Contains(msg, "failed to parse html") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "unexpected eof")
}

// isLargeFileURL checks if the URL points to a large file that should be handled differently.
func isLargeFileURL(url string) bool {
	url = strings.ToLower(url)
	return strings.HasSuffix(url, ".pdf") ||
		strings.HasSuffix(url, ".doc") ||
		strings.HasSuffix(url, ".docx") ||
		strings.HasSuffix(url, ".ppt") ||
		strings.HasSuffix(url, ".pptx") ||
		strings.Contains(url, "/pdf/") ||
		strings.Contains(url, "arxiv.org/pdf/") ||
		strings.Contains(url, "openaccess.thecvf.com")
}

// getTimeoutForURL returns appropriate timeout based on URL characteristics.
func getTimeoutForURL(url string) time.Duration {
	if isLargeFileURL(url) {
		return 30 * time.Second // Longer timeout for large files
	}
	return 15 * time.Second // Standard timeout for regular web pages
}

// doRequestWithTimeouts performs GET with progressive timeouts and simple retries.
func doRequestWithTimeouts(url string, ua string) (*http.Response, error) {
	retryTimeouts := getTimeoutsForURL(url)
	var lastErr error
	for i, to := range retryTimeouts {
		client := &http.Client{
			Timeout: to,
			Transport: &http.Transport{
				ForceAttemptHTTP2:      false,
				MaxResponseHeaderBytes: 1024 * 1024, // 1MB limit
				MaxIdleConns:           100,
				MaxIdleConnsPerHost:    10,
				IdleConnTimeout:        90 * time.Second,
			},
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 5 {
					return http.ErrUseLastResponse
				}
				return nil
			},
		}

		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Set("User-Agent", ua)

		resp, err := client.Do(req)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if !isRetryableTimeout(err) {
			break
		}
		// small backoff before next attempt
		if i < len(retryTimeouts)-1 {
			time.Sleep(200 * time.Millisecond)
		}
	}
	return nil, lastErr
}

// ExtractMetadataWithRetry performs the entire extraction process with retry logic.
func ExtractMetadataWithRetry(url1 string) (*models.Web_Image_MetaData, error) {
	// Handle large files with minimal processing
	if isLargeFileURL(url1) {
		return &models.Web_Image_MetaData{
			WebSource:   webSourceFromUrl(url1),
			Description: "Document (PDF/File)",
			Favicon:     fmt.Sprintf("https://t3.gstatic.com/faviconV2?client=SOCIAL&type=FAVICON&url=%s&size=32", url1),
		}, nil
	}

	// Retry the entire extraction process
	maxRetries := 3
	baseDelay := 1 * time.Second

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		result, err := ExtractMetadata(url1)
		if err == nil {
			return result, nil
		}

		lastErr = err
		// Retry on parsing failures and timeouts
		if isRetryableParsingError(err) && attempt < maxRetries-1 {
			delay := time.Duration(attempt+1) * baseDelay
			time.Sleep(delay)
			continue
		}

		// Don't retry on non-retryable errors (404, 403, etc.)
		if !isRetryableParsingError(err) {
			break
		}
	}

	return nil, lastErr
}

func ExtractMetadata(url1 string) (*models.Web_Image_MetaData, error) {
	//start := time.Now()

	ua := "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
	resp, err := doRequestWithTimeouts(url1, ua)
	if err != nil {
		return nil, fmt.Errorf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch URL, status: %d", resp.StatusCode)
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse HTML: %v", err)
	}

	data := &models.Web_Image_MetaData{}
	// 🧠 Extract website name
	if siteName, exists := doc.Find(`meta[property="og:site_name"]`).Attr("content"); exists {
		data.WebSource = strings.TrimSpace(siteName)
		fmt.Println("using the og:site_name as the name-->", data.WebSource)
		// } else if title := doc.Find("title").Text(); title != "" {
		// 	data.Name = strings.TrimSpace(title)
		// 	fmt.Println("using the tile as the name-->", data.Name)
	} else {
		// Fallback: extract from domain name
		data.WebSource = webSourceFromUrl(url1)
	}
	if desc, exists := doc.Find(`meta[name="description"]`).Attr("content"); exists {
		data.Description = strings.TrimSpace(desc)
	} else if ogDesc, exists := doc.Find(`meta[property="og:description"]`).Attr("content"); exists {
		data.Description = strings.TrimSpace(ogDesc)
	}

	if icon, exists := doc.Find(`link[rel="icon"]`).Attr("href"); exists {
		data.Favicon = resolveURL(url1, icon)
	} else if shortcut, exists := doc.Find(`link[rel="shortcut icon"]`).Attr("href"); exists {
		data.Favicon = resolveURL(url1, shortcut)
	} else {
		data.Favicon = fmt.Sprintf("https://t3.gstatic.com/faviconV2?client=SOCIAL&type=FAVICON&url=%s&size=32", url1)
		fmt.Println("hitting our default scrapper-->", data.Favicon)
	}
	//fmt.Println("data.Favicon-->", data.Favicon)
	return data, nil
}

func resolveURL(base, ref string) string {
	if strings.HasPrefix(ref, "http") {
		return ref
	}
	if strings.HasPrefix(ref, "//") {
		return "https:" + ref
	}
	if strings.HasPrefix(ref, "/") {
		if u, err := http.NewRequest("GET", base, nil); err == nil {
			return u.URL.Scheme + "://" + u.URL.Host + ref
		}
	}
	return ref
}
func webSourceFromUrl(url1 string) string {
	// Handle empty or invalid input
	if strings.TrimSpace(url1) == "" {
		return ""
	}

	parsedURL, err := url.Parse(url1)
	if err != nil {
		// If URL parsing fails, try to extract domain from the string directly
		// Remove common prefixes and suffixes
		clean := strings.TrimSpace(url1)
		clean = strings.TrimPrefix(clean, "http://")
		clean = strings.TrimPrefix(clean, "https://")
		clean = strings.TrimPrefix(clean, "www.")
		clean = strings.TrimPrefix(clean, "m.")
		clean = strings.TrimPrefix(clean, "edition.")

		// Remove path and query parameters
		if idx := strings.Index(clean, "/"); idx != -1 {
			clean = clean[:idx]
		}
		if idx := strings.Index(clean, "?"); idx != -1 {
			clean = clean[:idx]
		}
		if idx := strings.Index(clean, "#"); idx != -1 {
			clean = clean[:idx]
		}

		parts := strings.Split(clean, ".")
		if len(parts) >= 2 {
			return parts[len(parts)-2] // Second-level domain
		} else if len(parts) == 1 && parts[0] != "" {
			return parts[0] // Single domain
		}
		return ""
	}

	host := parsedURL.Hostname()
	if host == "" {
		return ""
	}

	// Clean up common prefixes
	host = strings.Replace(host, "www.", "", 1)
	host = strings.Replace(host, "m.", "", 1)
	host = strings.Replace(host, "edition.", "", 1)

	parts := strings.Split(host, ".")
	if len(parts) >= 2 {
		return parts[len(parts)-2] // Second-level domain (e.g., "google" from "google.com")
	} else if len(parts) == 1 && parts[0] != "" {
		return parts[0] // Single domain (e.g., "localhost")
	}

	return ""
}
