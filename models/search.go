package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"
)

type Search struct {
	SearchID       uuid.UUID    `db:"search_id" json:"search_id"`
	UserID         uuid.UUID    `db:"user_id" json:"user_id"`
	SystemPromptID *uuid.UUID   `db:"system_prompt_id" json:"system_prompt_id"`
	Keywords       pgtype.JSONB `db:"keywords" json:"keywords"`
	CreatedAt      time.Time    `db:"created_at" json:"created_at"`
}
