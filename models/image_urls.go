package models

type ImageURL struct {
	ImageURL   string `db:"image_url" json:"image_url"`
	ResponseID string `db:"response_id" json:"response_id"`
	IsCitation bool   `db:"is_citation" json:"is_citation"`
	SeqNo      *int   `db:"seq_no" json:"seq_no"`
}
