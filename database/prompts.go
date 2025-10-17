package database

import (
	"context"
	"fmt"

	"github.com/InvicttusGIT/BlinkAi_SearchEngineApps_Productivity_Backend/models"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LoadActivePromptsFromDB loads active system prompts from the database
func LoadActivePromptsFromDB(ctx context.Context, pool *pgxpool.Pool) (map[string][]models.PromptData, error) {
	prompts := make(map[string][]models.PromptData)

	// Query to get active prompts
	query := `
		SELECT id,search_type, prompt_text,platform 
		FROM system_prompt 
		WHERE is_active = true 
		ORDER BY search_type
	`

	rows, err := pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query system prompts: %w", err)
	}
	defer rows.Close()

	// Load prompts into the map

	for rows.Next() {
		var id uuid.UUID
		var searchType *string
		var promptText string
		var platform string

		if err := rows.Scan(&id, &searchType, &promptText, &platform); err != nil {
			return nil, fmt.Errorf("error scanning prompt row: %w", err)
		}

		// Handle NULL search_type (use "default" as key)
		key := "default"
		searchTypeStr := "default"
		if searchType != nil && *searchType != "" {
			key = *searchType
			searchTypeStr = *searchType
		}

		prompts[key] = append(prompts[key], models.PromptData{
			ID:         id,
			SearchType: searchTypeStr,
			PromptText: promptText,
			Platform:   platform,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating prompt rows: %w", err)
	}

	return prompts, nil
}
