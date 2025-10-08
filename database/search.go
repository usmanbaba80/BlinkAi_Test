package database

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/InvicttusGIT/BlinkAi_SearchEngineApps_Productivity_Backend/models"
	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CreateSearch stores a new search request in the database
func CreateSearch(ctx context.Context, pool *pgxpool.Pool, userID, systemPromptID string, keywords []string) (*models.Search, error) {
	// Parse userID to UUID
	userUUID, err := uuid.Parse(userID)
	if err != nil {
		return nil, fmt.Errorf("invalid user ID format: %w", err)
	}

	// Parse systemPromptID to UUID (can be nil)
	var systemPromptUUID *uuid.UUID
	if systemPromptID != "" {
		parsedPromptID, err := uuid.Parse(systemPromptID)
		if err != nil {
			return nil, fmt.Errorf("invalid system prompt ID format: %w", err)
		}
		systemPromptUUID = &parsedPromptID
	}

	// Convert keywords to JSONB
	keywordsJSON, err := json.Marshal(keywords)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal keywords to JSON: %w", err)
	}

	var keywordsJSONB pgtype.JSONB
	if err := keywordsJSONB.Set(keywordsJSON); err != nil {
		return nil, fmt.Errorf("failed to set keywords JSONB: %w", err)
	}

	// Generate new search ID
	searchID := uuid.New()

	query := `
		INSERT INTO searches (search_id, user_id, system_prompt_id, keywords, created_at)
		VALUES ($1, $2, $3, $4, NOW())
		RETURNING search_id, user_id, system_prompt_id, keywords, created_at
	`

	var search models.Search
	err = pool.QueryRow(ctx, query, searchID, userUUID, systemPromptUUID, keywordsJSONB).Scan(
		&search.SearchID,
		&search.UserID,
		&search.SystemPromptID,
		&search.Keywords,
		&search.CreatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create search: %w", err)
	}

	return &search, nil
}

// GetSearchByID retrieves a search by its ID
func GetSearchByID(ctx context.Context, pool *pgxpool.Pool, searchID string) (*models.Search, error) {
	// Parse searchID to UUID
	searchUUID, err := uuid.Parse(searchID)
	if err != nil {
		return nil, fmt.Errorf("invalid search ID format: %w", err)
	}

	query := `
		SELECT search_id, user_id, system_prompt_id, keywords, created_at 
		FROM searches 
		WHERE search_id = $1
	`

	var search models.Search
	err = pool.QueryRow(ctx, query, searchUUID).Scan(
		&search.SearchID,
		&search.UserID,
		&search.SystemPromptID,
		&search.Keywords,
		&search.CreatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get search by ID: %w", err)
	}

	return &search, nil
}

// GetSearchesByUserID retrieves all searches for a specific user
func GetSearchesByUserID(ctx context.Context, pool *pgxpool.Pool, userID string, limit int) ([]models.Search, error) {
	// Parse userID to UUID
	userUUID, err := uuid.Parse(userID)
	if err != nil {
		return nil, fmt.Errorf("invalid user ID format: %w", err)
	}

	query := `
		SELECT search_id, user_id, system_prompt_id, keywords, created_at 
		FROM searches 
		WHERE user_id = $1 
		ORDER BY created_at DESC 
		LIMIT $2
	`

	rows, err := pool.Query(ctx, query, userUUID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query searches by user ID: %w", err)
	}
	defer rows.Close()

	var searches []models.Search
	for rows.Next() {
		var search models.Search
		err := rows.Scan(
			&search.SearchID,
			&search.UserID,
			&search.SystemPromptID,
			&search.Keywords,
			&search.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("error scanning search row: %w", err)
		}
		searches = append(searches, search)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating search rows: %w", err)
	}

	return searches, nil
}
