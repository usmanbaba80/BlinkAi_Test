package models

// ChatMessage represents a single message for Perplexity API
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// PerplexityRequest is the request body we send to Perplexity
type PerplexityRequest struct {
	Model                  string        `json:"model"`
	Messages               []ChatMessage `json:"messages"`
	Stream                 bool          `json:"stream"`
	MediaResponse          interface{}   `json:"media_response"`
	ReturnRelatedQuestions bool          `json:"return_related_questions"`
	SearchDomainFilter     []string      `json:"search_domain_filter"`
	Temperature            float32       `json:"temperature"`
	MaxSearchResults       int           `json:"max_search_results"`
}

// PerplexityChunk models the streaming SSE chunk from Perplexity
type PerplexityChunk struct {
	ID      string `json:"id,omitempty"`
	Model   string `json:"model,omitempty"`
	Created int64  `json:"created,omitempty"`
	Object  string `json:"object"`

	Usage *struct {
		PromptTokens      int    `json:"prompt_tokens"`
		CompletionTokens  int    `json:"completion_tokens"`
		TotalTokens       int    `json:"total_tokens"`
		SearchContextSize string `json:"search_context_size"`
	} `json:"usage,omitempty"`

	Citations []string `json:"citations,omitempty"`

	SearchResults []struct {
		Title       string  `json:"title"`
		URL         string  `json:"url"`
		Date        string  `json:"date"`
		LastUpdated *string `json:"last_updated"`
		Snippet     string  `json:"snippet"`
		Source      string  `json:"source"`
	} `json:"search_results,omitempty"`

	Videos []struct {
		URL             string `json:"url"`
		ThumbnailWidth  int    `json:"thumbnail_width"`
		ThumbnailHeight int    `json:"thumbnail_height"`
		ThumbnailURL    string `json:"thumbnail_url"`
	} `json:"videos,omitempty"`

	Images []struct {
		ImageURL  string `json:"image_url"`
		OriginURL string `json:"origin_url"`
		Height    int    `json:"height"`
		Width     int    `json:"width"`
		Title     string `json:"title"`
	} `json:"images,omitempty"`

	RelatedQuestions []string `json:"related_questions,omitempty"`

	Choices []struct {
		Index        int     `json:"index"`
		FinishReason *string `json:"finish_reason"`
		Message      *struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"message"`
		Delta *struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"delta"`
	} `json:"choices"`
}
