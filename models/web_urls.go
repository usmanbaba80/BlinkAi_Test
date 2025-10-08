package models

type WebURL struct {
	WebURL     string `db:"web_url" json:"web_url"`
	ResponseID string `db:"response_id" json:"response_id"`
	IsCitation bool   `db:"is_citation" json:"is_citation"`
	SeqNo      *int   `db:"seq_no" json:"seq_no"`
}
