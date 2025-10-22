package models

type ImageMetadata struct {
	URL                string  `db:"url" json:"url"`
	URLImage           *string `db:"url_image" json:"url_image"`
	AIOverview         *string `db:"ai_overview" json:"ai_overview"`
	SnippetDescription *string `db:"snippet_description" json:"snippet_description"`
	Title              *string `db:"title" json:"title"`
	Favicon            *string `db:"favicon" json:"favicon"`
	Source             *string `db:"source" json:"source"`
	WebSource          *string `db:"web_source" json:"web_source"`
}
