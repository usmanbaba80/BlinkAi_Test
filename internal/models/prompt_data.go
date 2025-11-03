package models

import (
	"github.com/google/uuid"
)

// PromptData holds the prompt information including ID
type PromptData struct {
	ID         uuid.UUID `json:"id"`
	SearchType string    `json:"search_type"`
	PromptText string    `json:"prompt_text"`
	Platform   string    `json:"platform"`
}

