package models

import "time"

// ImageResponse represents the API response for image search
type ImageResponse struct {
	Source        string        `json:"source"`
	Query         string        `json:"query"`
	Location      string        `json:"location"`
	TotalResults  int           `json:"total_results"`
	Start         int           `json:"start"`
	End           int           `json:"end"`
	ReturnedCount int           `json:"returned_count"`
	ImagesResults []interface{} `json:"images_results"`
}

// QueryResult represents a record in the query_results table
type QueryResult struct {
	Idx        int       `json:"idx"`
	Query      string    `json:"query"`
	SearchType string    `json:"searchType"`
	Location   string    `json:"location"`
	Results    string    `json:"results"`
	Timestamp  time.Time `json:"timestamp"`
}

