package models

import (
	"time"

	"github.com/google/uuid"
)

type SystemPrompt struct {
	ID         uuid.UUID `db:"id" json:"id"`
	SearchType *string   `db:"search_type" json:"search_type"`
	PromptText string    `db:"prompt_text" json:"prompt_text"`
	CreatedAt  time.Time `db:"created_at" json:"created_at"`
	Version    *string   `db:"version" json:"version"`
	IsActive   bool      `db:"is_active" json:"is_active"`
}
