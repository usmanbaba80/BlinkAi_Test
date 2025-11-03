package models

import (
	"time"

	"github.com/google/uuid"
)

type VideoMetadata struct {
	VideoID     uuid.UUID  `db:"video_id" json:"video_id"`
	VideoURL    *string    `db:"video_url" json:"video_url"`
	Title       *string    `db:"title" json:"title,omitempty"`
	Thumbnail   *string    `db:"thumbnail" json:"thumbnail"`
	PublishDate *time.Time `db:"publish_date" json:"publish_date"`
	Likes       *int64     `db:"likes" json:"likes,omitempty"`
	Views       *int64     `db:"views" json:"views,omitempty"`
	Description *string    `db:"description" json:"description"`
}

