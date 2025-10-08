package models

import (
	"time"

	"github.com/google/uuid"
)

type Trend struct {
	ID        uuid.UUID `db:"id" json:"id"`
	Keyword   string    `db:"keyword" json:"keyword"`
	UpdatedAt time.Time `db:"updated_at" json:"updated_at"`
}
