package database

import (
	"context"
	"fmt"

	"github.com/InvicttusGIT/BlinkAi_Merged/internal/models"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// GetUserByID retrieves a user by their ID
func GetUserByID(ctx context.Context, pool *pgxpool.Pool, userID string) (*models.User, error) {
	userUUID, err := uuid.Parse(userID)
	if err != nil {
		return nil, fmt.Errorf("invalid user ID format: %w", err)
	}

	query := `
		SELECT id, email, device_id, platform_name, created_at 
		FROM users 
		WHERE id = $1
	`

	var user models.User
	err = pool.QueryRow(ctx, query, userUUID).Scan(
		&user.ID,
		&user.Email,
		&user.DeviceID,
		&user.PlatformName,
		&user.CreatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get user by ID: %w", err)
	}

	return &user, nil
}

// CreateUser creates a new user in the database
func CreateUser(ctx context.Context, pool *pgxpool.Pool, userID, deviceID, platformName string) (*models.User, error) {
	userUUID, err := uuid.Parse(userID)
	if err != nil {
		return nil, fmt.Errorf("invalid user ID format: %w", err)
	}

	var deviceUUID *uuid.UUID
	if deviceID != "" {
		parsedDeviceID, err := uuid.Parse(deviceID)
		if err != nil {
			return nil, fmt.Errorf("invalid device ID format: %w", err)
		}
		deviceUUID = &parsedDeviceID
	}

	query := `
		INSERT INTO users (id, device_id, platform_name, created_at)
		VALUES ($1, $2, $3, NOW())
		RETURNING id, email, device_id, platform_name, created_at
	`

	var user models.User
	err = pool.QueryRow(ctx, query, userUUID, deviceUUID, platformName).Scan(
		&user.ID,
		&user.Email,
		&user.DeviceID,
		&user.PlatformName,
		&user.CreatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create user: %w", err)
	}

	return &user, nil
}

// GetOrCreateUser gets an existing user or creates a new one
func GetOrCreateUser(ctx context.Context, pool *pgxpool.Pool, userID, deviceID, platformName string) (*models.User, error) {
	user, err := GetUserByID(ctx, pool, userID)
	if err == nil {
		return user, nil
	}

	user, err = CreateUser(ctx, pool, userID, deviceID, platformName)
	if err != nil {
		return nil, fmt.Errorf("failed to get or create user: %w", err)
	}

	return user, nil
}

