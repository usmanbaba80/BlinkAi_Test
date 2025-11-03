package repository

import (
	"database/sql"
	"encoding/json"
	"log"
)

// ImagesRepository handles database operations for image searches
type ImagesRepository struct {
	db *sql.DB
}

// NewImagesRepository creates a new ImagesRepository
func NewImagesRepository(db *sql.DB) *ImagesRepository {
	return &ImagesRepository{db: db}
}

// GetCachedImageResults checks if we have cached results for a query and location
// Returns the images_results array if found, nil if not found
func (r *ImagesRepository) GetCachedImageResults(query string, location string) ([]interface{}, error) {
	if r.db == nil {
		return nil, nil
	}

	log.Printf("Checking database for cached image results - query: %s, location: %s", query, location)

	var resultsData string
	err := r.db.QueryRow(`
		SELECT results 
		FROM query_results 
		WHERE query = $1 AND location = $2 
		ORDER BY timestamp DESC 
		LIMIT 1
	`, query, location).Scan(&resultsData)

	if err != nil {
		if err == sql.ErrNoRows {
			log.Printf("No cached results found for query: %s, location: %s", query, location)
			return nil, nil
		}
		log.Printf("Database query error: %v", err)
		return nil, err
	}

	log.Printf("Found cached results in database")

	// Parse the full response JSON to extract images_results
	var fullResponse map[string]interface{}
	if err := json.Unmarshal([]byte(resultsData), &fullResponse); err != nil {
		log.Printf("Failed to unmarshal cached results: %v", err)
		return nil, err
	}

	// Extract images_results array
	if imagesResults, ok := fullResponse["images_results"].([]interface{}); ok {
		log.Printf("Found %d cached image results", len(imagesResults))
		return imagesResults, nil
	}

	log.Printf("No images_results field found in cached data")
	return nil, nil
}

// SaveImageResults stores the full API response to database in the background
func (r *ImagesRepository) SaveImageResults(query string, searchType string, location string, fullResponse []byte) {
	go func() {
		if r.db == nil {
			log.Println("DB is nil, cannot save image results")
			return
		}

		log.Printf("Saving image results to database - query: %s, location: %s", query, location)
		_, err := r.db.Exec(`
			INSERT INTO query_results (query, searchType, location, results) 
			VALUES ($1, $2, $3, $4)
		`, query, searchType, location, string(fullResponse))

		if err != nil {
			log.Printf("Failed to save image results to database: %v", err)
			return
		}
		log.Printf("Successfully saved image results to database")
	}()
}

