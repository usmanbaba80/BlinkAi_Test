package models

// CombinedItem is a unified entry used for printing/storing
// while preserving the original source-specific payload.
type CombinedItem struct {
	SeqNo      int         `json:"seq_no"`
	IsCitation bool        `json:"is_citation"`
	Kind       string      `json:"kind"` // "web" | "image" | "video" | "related"
	Payload    interface{} `json:"payload"`
}

type WebItem struct {
	AIOverview  string  `json:"ai_overview"`
	Title       string  `json:"title"`
	URL         string  `json:"url"`
	Date        string  `json:"date"`
	LastUpdated *string `json:"last_updated"`
	Snippet     string  `json:"snippet"`
	Source      string  `json:"source"`
	Favicon     string  `json:"favicon"`
	WebSource   string  `json:"web_source"`
	Description string  `json:"description"`
}

type ImageItem struct {
	AIOverview string `json:"ai_overview"`
	ImageURL   string `json:"image_url"`
	OriginURL  string `json:"origin_url"`
	Height     int    `json:"height"`
	Width      int    `json:"width"`
	Title      string `json:"title"`
	Snippet    string `json:"snippet"`
	Favicon    string `json:"favicon"`
	Source     string `json:"source"`
	WebSource  string `json:"web_source"`
}

type VideoItem struct {
	VideoID         string `json:"video_id"`
	URL             string `json:"url"`
	ThumbnailWidth  int    `json:"thumbnail_width"`
	ThumbnailHeight int    `json:"thumbnail_height"`
	ThumbnailURL    string `json:"thumbnail_url"`
	Title           string `json:"title"`
	//Thumbnail       string `json:"thumbnail"`
	PublishDate    string `json:"publish_date"`
	Likes          int    `json:"likes"`
	Views          int    `json:"views"`
	Description    string `json:"description"`
	IsNotProcessed bool   `json:"is_not_processed"`
}

// RelatedList represents all related searches grouped into a single item
type RelatedList struct {
	Queries []string `json:"queries"`
}

