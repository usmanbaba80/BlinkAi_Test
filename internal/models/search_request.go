package models

// SearchRequest represents the request structure for the search endpoint
type SearchRequest struct {
	Keywords     []string `json:"keywords" validate:"required"`
	UserID       string   `json:"user_id" validate:"required"`
	DeviceID     string   `json:"device_id" validate:"required"`
	PlatformName string   `json:"platform_name" validate:"required"`
	SearchType   string   `json:"search_type" validate:"required"`
}

// SearchRequestResponse represents the response structure for the search endpoint
type SearchRequestResponse struct {
	Success        bool     `json:"success"`
	Message        string   `json:"message"`
	SearchID       string   `json:"search_id,omitempty"`
	Keywords       []string `json:"keywords"`
	UserID         string   `json:"user_id"`
	DeviceID       string   `json:"device_id"`
	PlatformName   string   `json:"platform_name"`
	SearchType     string   `json:"search_type"`
	SystemPromptID string   `json:"system_prompt_id,omitempty"`
	SystemPrompt   string   `json:"system_prompt,omitempty"`
	Timestamp      string   `json:"timestamp"`
}

