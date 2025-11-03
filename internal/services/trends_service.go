package services

import (
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/InvicttusGIT/BlinkAi_Merged/internal/repository"
)

// TrendsService handles business logic for trends
type TrendsService struct {
	repo   *repository.TrendsRepository
	apiKey string
}

// NewTrendsService creates a new TrendsService
func NewTrendsService(repo *repository.TrendsRepository, apiKey string) *TrendsService {
	return &TrendsService{
		repo:   repo,
		apiKey: apiKey,
	}
}

// GetRandomTrends gets 3 random trends from cache or API
func (s *TrendsService) GetRandomTrends(location string, params map[string]string) ([]interface{}, string, error) {
	// Step 1: Check database for cached trends
	allTrends, err := s.repo.GetRandomTrends(location)
	if err != nil {
		log.Printf("Error fetching from database: %v", err)
	}

	// Step 2: If cached data exists, return 3 random items
	if allTrends != nil && len(allTrends) > 0 {
		randomTrends := s.selectRandomTrends(allTrends, 3)
		log.Printf("Returning %d random trends from database", len(randomTrends))
		return randomTrends, "database", nil
	}

	// Step 3: No cached data, fetch from API
	log.Println("No data in database, fetching from ScrapingDog API")
	return s.fetchFromAPI(location, params)
}

// fetchFromAPI fetches trends from ScrapingDog API
func (s *TrendsService) fetchFromAPI(location string, params map[string]string) ([]interface{}, string, error) {
	requestURL := s.buildTrendsURL(params)

	// Use retry logic with exponential backoff for 5xx and 429 errors
	config := DefaultRetryConfig()
	resp, err := retryHTTPGet(requestURL, config)
	if err != nil {
		log.Printf("ScrapingDog trends API failed after retries: %v", err)
		return nil, "api", err
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("error reading response: %v", err)
		return nil, "api", err
	}

	// Parse JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &parsed); err != nil {
		log.Printf("Failed to parse response JSON: %v", err)
		return nil, "api", err
	}

	// Extract trending_searches
	if raw, ok := parsed["trending_searches"].([]interface{}); ok {
		log.Printf("Found %d trending searches", len(raw))
		
		// Keep top 10
		top := raw
		if len(raw) > 10 {
			top = raw[:10]
			log.Printf("Limiting to top 10 results")
		}

		// Save to database
		topJSON, mErr := json.Marshal(top)
		if mErr == nil {
			log.Printf("Marshaled top %d trends to JSON", len(top))
			if saveErr := s.repo.SaveTrends(topJSON, location); saveErr != nil {
				log.Printf("save error: %v", saveErr)
			}
		}

		// Return 3 random items from top 10
		randomTrends := s.selectRandomTrends(top, 3)
		log.Printf("Returning %d random trends from API response", len(randomTrends))
		return randomTrends, "api", nil
	}

	log.Println("No 'trending_searches' field found in response")
	return nil, "api", err
}

// selectRandomTrends selects N random items from a slice
func (s *TrendsService) selectRandomTrends(trends []interface{}, count int) []interface{} {
	if len(trends) <= count {
		return trends
	}

	rand.Seed(time.Now().UnixNano())
	selected := make([]interface{}, 0, count)
	indices := rand.Perm(len(trends))[:count]

	for _, idx := range indices {
		selected = append(selected, trends[idx])
	}

	return selected
}

// buildTrendsURL constructs the ScrapingDog Google Trends API URL
func (s *TrendsService) buildTrendsURL(params map[string]string) string {
	var sb strings.Builder
	sb.WriteString("https://api.scrapingdog.com/google_trends/trending_now?")

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

