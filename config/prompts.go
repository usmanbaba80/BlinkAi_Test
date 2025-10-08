package config

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/InvicttusGIT/BlinkAi_SearchEngineApps_Productivity_Backend/database"
	"github.com/InvicttusGIT/BlinkAi_SearchEngineApps_Productivity_Backend/models"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PromptsConfig holds the loaded system prompts
type PromptsConfig struct {
	mu      sync.RWMutex
	prompts map[string]models.PromptData // key: search_type, value: PromptData
}

// Global instance
var SystemPrompts *PromptsConfig

// NewPromptsConfig creates a new PromptsConfig instance
func NewPromptsConfig() *PromptsConfig {
	return &PromptsConfig{
		prompts: make(map[string]models.PromptData),
	}
}

// LoadPrompts loads active system prompts from the database into memory
func (pc *PromptsConfig) LoadPrompts(ctx context.Context, pool *pgxpool.Pool) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	// Load prompts from database
	prompts, err := database.LoadActivePromptsFromDB(ctx, pool)
	if err != nil {
		return fmt.Errorf("failed to load prompts from database: %w", err)
	}

	// Store in memory
	pc.prompts = prompts

	// Log loaded prompts
	for key := range prompts {
		log.Printf("Loaded prompt for search_type: %s", key)
	}

	log.Printf("Successfully loaded %d active system prompts into memory", len(pc.prompts))
	return nil
}

// GetPrompt retrieves a prompt by search type
func (pc *PromptsConfig) GetPrompt(searchType string) (models.PromptData, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	// Try to get the specific search type
	if prompt, exists := pc.prompts[searchType]; exists {
		return prompt, true
	}

	// Fallback to default if specific type not found
	if prompt, exists := pc.prompts["default"]; exists {
		return prompt, true
	}

	return models.PromptData{}, false
}

// GetAllPrompts returns a copy of all loaded prompts
func (pc *PromptsConfig) GetAllPrompts() map[string]models.PromptData {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	// Create a copy to avoid race conditions
	prompts := make(map[string]models.PromptData)
	for k, v := range pc.prompts {
		prompts[k] = v
	}
	return prompts
}

// ReloadPrompts reloads prompts from the database
func (pc *PromptsConfig) ReloadPrompts(ctx context.Context, pool *pgxpool.Pool) error {
	log.Println("Reloading system prompts...")
	return pc.LoadPrompts(ctx, pool)
}

// InitializePrompts initializes the global prompts configuration
func InitializePrompts(ctx context.Context, pool *pgxpool.Pool) error {
	SystemPrompts = NewPromptsConfig()
	return SystemPrompts.LoadPrompts(ctx, pool)
}
