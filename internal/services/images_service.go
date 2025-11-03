package services

import (
	"encoding/json"
	"io"
	"log"
	"strings"

	"github.com/InvicttusGIT/BlinkAi_Merged/internal/repository"
)

// ImagesService handles business logic for image searches
type ImagesService struct {
	repo   *repository.ImagesRepository
	apiKey string
}

// NewImagesService creates a new ImagesService
func NewImagesService(repo *repository.ImagesRepository, apiKey string) *ImagesService {
	return &ImagesService{
		repo:   repo,
		apiKey: apiKey,
	}
}

// SearchImages searches for images either from cache or API
func (s *ImagesService) SearchImages(query, location string, start, end int, params map[string]string) ([]interface{}, int, string, error) {
	// Step 1: Check cache
	cachedResults, err := s.repo.GetCachedImageResults(query, location)
	if err != nil {
		log.Printf("Error checking cache: %v", err)
	}

	// Step 2: If cached results found, return paginated data
	if cachedResults != nil && len(cachedResults) > 0 {
		log.Printf("Returning paginated results from cache (total: %d)", len(cachedResults))
		paginatedResults := paginateResults(cachedResults, start, end)
		return paginatedResults, len(cachedResults), "cache", nil
	}

	// Step 3: No cached data, fetch from API
	log.Printf("No cached results found, calling ScrapingDog API")
	return s.fetchFromAPI(query, location, start, end, params)
}

// fetchFromAPI fetches images from ScrapingDog API
func (s *ImagesService) fetchFromAPI(query, location string, start, end int, params map[string]string) ([]interface{}, int, string, error) {
	requestURL := s.buildImagesURL(params)

	// Use retry logic with exponential backoff for 5xx and 429 errors
	config := DefaultRetryConfig()
	resp, err := retryHTTPGet(requestURL, config)
	if err != nil {
		log.Printf("ScrapingDog images API failed after retries: %v", err)
		return nil, 0, "api", err
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("error reading response: %v", err)
		return nil, 0, "api", err
	}

	log.Printf("Successfully fetched images from API, size: %d bytes", len(bodyBytes))

	// Parse response
	var parsed map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &parsed); err != nil {
		log.Printf("Failed to parse API response: %v", err)
		return nil, 0, "api", err
	}

	// Save full response to database in background
	searchType := "image_search"
	if st, ok := parsed["search_type"].(string); ok {
		searchType = st
	}
	s.repo.SaveImageResults(query, searchType, location, bodyBytes)

	// Extract images_results for pagination
	if imagesResults, ok := parsed["images_results"].([]interface{}); ok {
		log.Printf("Found %d images in API response", len(imagesResults))
		paginatedResults := paginateResults(imagesResults, start, end)
		return paginatedResults, len(imagesResults), "api", nil
	}

	log.Printf("Failed to extract images_results from API response")
	return nil, 0, "api", nil
}

// buildImagesURL constructs the ScrapingDog Google Images API URL
func (s *ImagesService) buildImagesURL(params map[string]string) string {
	var sb strings.Builder
	sb.WriteString("https://api.scrapingdog.com/google_images/?")

	sb.WriteString("api_key=")
	sb.WriteString(s.apiKey)

	for k, v := range params {
		if v == "" || k == "api_key" {
			continue
		}
		sb.WriteString("&")
		sb.WriteString(urlQueryEscape(k))
		sb.WriteString("=")
		sb.WriteString(urlQueryEscape(v))
	}

	return sb.String()
}

// paginateResults returns a slice of results based on start and end indices
func paginateResults(results []interface{}, start int, end int) []interface{} {
	total := len(results)

	// Validate and adjust start
	if start < 0 {
		start = 0
	}
	if start >= total {
		return []interface{}{}
	}

	// Validate and adjust end
	if end <= 0 || end > total {
		end = total
	}
	if end <= start {
		return []interface{}{}
	}

	return results[start:end]
}

