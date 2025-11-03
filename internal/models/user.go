package models

import (
	"time"

	"github.com/google/uuid"
)

type User struct {
	ID           uuid.UUID  `db:"id" json:"id"`
	Email        *string    `db:"email" json:"email"`
	DeviceID     *uuid.UUID `db:"device_id" json:"device_id"`
	PlatformName string     `db:"platform_name" json:"platform_name"`
	CreatedAt    time.Time  `db:"created_at" json:"created_at"`
}

