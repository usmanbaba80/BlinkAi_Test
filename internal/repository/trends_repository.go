package repository

import (
	"database/sql"
	"encoding/json"
	"log"
)

// TrendsRepository handles database operations for trends
type TrendsRepository struct {
	db *sql.DB
}

// NewTrendsRepository creates a new TrendsRepository
func NewTrendsRepository(db *sql.DB) *TrendsRepository {
	return &TrendsRepository{db: db}
}

// SaveTrends stores top trends JSON and geo into trends_now
func (r *TrendsRepository) SaveTrends(trendsJSON []byte, location string) error {
	if r.db == nil {
		log.Println("DB is nil, cannot save trends")
		return nil
	}

	log.Printf("Saving trends to database for location: %s", location)
	_, err := r.db.Exec(`INSERT INTO trends_now (trends_data, location) VALUES ($1, $2)`, string(trendsJSON), location)
	if err != nil {
		log.Printf("Failed to insert into database: %v", err)
		return err
	}

	log.Printf("Successfully saved %d bytes of trends data to database", len(trendsJSON))
	return nil
}

// GetRandomTrends fetches the most recent trends for a location
func (r *TrendsRepository) GetRandomTrends(location string) ([]interface{}, error) {
	if r.db == nil {
		return nil, nil
	}

	log.Printf("Checking database for existing trends for location: %s", location)

	var trendsData string
	err := r.db.QueryRow(`
		SELECT trends_data 
		FROM trends_now 
		WHERE location = $1 
		ORDER BY timestamp DESC 
		LIMIT 1
	`, location).Scan(&trendsData)

	if err != nil {
		if err == sql.ErrNoRows {
			log.Printf("No existing trends found in database for location: %s", location)
			return nil, nil
		}
		log.Printf("Database query error: %v", err)
		return nil, err
	}

	log.Printf("Found existing trends in database for location: %s", location)

	// Parse the JSON array
	var allTrends []interface{}
	if err := json.Unmarshal([]byte(trendsData), &allTrends); err != nil {
		log.Printf("Failed to unmarshal trends data: %v", err)
		return nil, err
	}

	log.Printf("Total trends in database: %d", len(allTrends))
	return allTrends, nil
}
