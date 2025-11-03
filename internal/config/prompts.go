package config

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/InvicttusGIT/BlinkAi_Merged/internal/database"
	"github.com/InvicttusGIT/BlinkAi_Merged/internal/models"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PromptsConfig holds the loaded system prompts
type PromptsConfig struct {
	mu      sync.RWMutex
	prompts map[string][]models.PromptData // key: search_type, value: PromptData
}

// Global instance
var SystemPrompts *PromptsConfig

// NewPromptsConfig creates a new PromptsConfig instance
func NewPromptsConfig() *PromptsConfig {
	return &PromptsConfig{
		prompts: make(map[string][]models.PromptData),
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
func (pc *PromptsConfig) GetPrompt(searchType string, platformName string) (models.PromptData, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	platformLower := strings.ToLower(strings.TrimSpace(platformName))

	// Try to get prompts for the specific search type
	if promptsForType, exists := pc.prompts[searchType]; exists {
		// 1) Exact (case-insensitive) platform match
		for _, p := range promptsForType {
			if strings.ToLower(strings.TrimSpace(p.Platform)) == platformLower {
				return p, true
			}
		}
		// 2) Fallback to platform-agnostic entry within same search type (empty platform)
		for _, p := range promptsForType {
			if strings.TrimSpace(p.Platform) == "" {
				return p, true
			}
		}
	}

	// 3) Global default fallback (ensure slice non-empty)
	if defaults, exists := pc.prompts["default"]; exists {
		if len(defaults) > 0 {
			return defaults[0], true
		}
	}

	return models.PromptData{}, false
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

