package models

type WebMetadata struct {
	URL                string  `db:"url" json:"url"`
	AIOverview         *string `db:"ai_overview" json:"ai_overview"`
	SnippetDescription *string `db:"snippet_description" json:"snippet_description,"`
	Title              *string `db:"title" json:"title"`
	Favicon            *string `db:"favicon" json:"favicon"`
	Source             *string `db:"source" json:"source"`
}
