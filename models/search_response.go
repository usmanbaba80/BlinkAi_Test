package models

import (
	"github.com/google/uuid"
)

type SearchResponse struct {
	ResponseID uuid.UUID `db:"response_id" json:"response_id"`
	SearchID   uuid.UUID `db:"search_id" json:"search_id"`
	AISummary  *string   `db:"ai_summary" json:"ai_summary"`
}
