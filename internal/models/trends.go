package models

import "time"

// TrendResponse represents the API response for trends
type TrendResponse struct {
	Source           string        `json:"source"`
	Location         string        `json:"location"`
	Count            int           `json:"count"`
	TrendingSearches []interface{} `json:"trending_searches"`
	StatusCode       int           `json:"status_code"`
	Success          bool          `json:"success"`
}

// TrendRecord represents a record in the trends_now table
type TrendRecord struct {
	ID         int       `json:"id"`
	TrendsData string    `json:"trends_data"`
	Location   string    `json:"location"`
	CreatedAt  time.Time `json:"created_at"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error      string `json:"error"`
	StatusCode int    `json:"status_code"`
	Success    bool   `json:"success"`
}

