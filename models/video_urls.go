package models

import (
	"github.com/google/uuid"
)

type VideoURL struct {
	VideoID    uuid.UUID `db:"video_id" json:"video_id"`
	ResponseID uuid.UUID `db:"response_id" json:"response_id"`
	IsCitation bool      `db:"is_citation" json:"is_citation"`
	SeqNo      *int      `db:"seq_no" json:"seq_no"`
}
