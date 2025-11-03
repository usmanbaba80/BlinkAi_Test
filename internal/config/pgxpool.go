package config

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect initializes a PostgreSQL connection pool using environment variables.
// Expected env vars: DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME.
func Connect(ctx context.Context) (*pgxpool.Pool, error) {
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbName := os.Getenv("DB_NAME")

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", dbUser, dbPassword, dbHost, dbPort, dbName)

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse pg config: %w", err)
	}

	// Sensible pool defaults
	cfg.MaxConns = 10 ///we can add the number of connections here like 30-40
	cfg.MinConns = 1
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pg pool: %w", err)
	}

	// Verify connection
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping pg: %w", err)
	}

	log.Println("PostgreSQL connection established")

	// Initialize tables
	if err := initTablesForPool(ctx, pool); err != nil {
		log.Printf("Warning: Failed to initialize tables: %v", err)
	}

	return pool, nil
}

// initTablesForPool creates all necessary tables for pgxpool
func initTablesForPool(ctx context.Context, pool *pgxpool.Pool) error {
	// Table: users
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS users (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			email VARCHAR(255),
			device_id UUID,
			platform_name VARCHAR(255),
			created_at TIMESTAMP DEFAULT NOW()
		);
	`); err != nil {
		return fmt.Errorf("create users table: %w", err)
	}
	log.Println("Table 'users' ready")

	// Table: system_prompt
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS system_prompt (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			search_type VARCHAR(50),
			prompt_text TEXT,
			version VARCHAR(20),
			platform VARCHAR(50),
			is_active BOOLEAN DEFAULT TRUE,
			created_at TIMESTAMP DEFAULT NOW()
		);
	`); err != nil {
		return fmt.Errorf("create system_prompt table: %w", err)
	}
	log.Println("Table 'system_prompt' ready")

	// Table: searches
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS searches (
			search_id UUID PRIMARY KEY,
			user_id UUID REFERENCES users(id),
			system_prompt_id UUID REFERENCES system_prompt(id),
			keywords JSONB,
			created_at TIMESTAMP DEFAULT NOW()
		);
	`); err != nil {
		return fmt.Errorf("create searches table: %w", err)
	}
	log.Println("Table 'searches' ready")

	// Table: search_response
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS search_response (
			response_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			search_id UUID REFERENCES searches(search_id),
			ai_summary TEXT,
			created_at TIMESTAMP DEFAULT NOW()
		);
	`); err != nil {
		return fmt.Errorf("create search_response table: %w", err)
	}
	log.Println("Table 'search_response' ready")

	// Table: web_urls
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS web_urls (
			web_url TEXT PRIMARY KEY,
			response_id UUID REFERENCES search_response(response_id),
			is_citation BOOLEAN,
			seq_no INT,
			created_at TIMESTAMP DEFAULT NOW()
		);
	`); err != nil {
		return fmt.Errorf("create web_urls table: %w", err)
	}
	log.Println("Table 'web_urls' ready")

	// Table: video_urls
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS video_urls (
			video_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			response_id UUID REFERENCES search_response(response_id),
			is_citation BOOLEAN,
			seq_no INT,
			created_at TIMESTAMP DEFAULT NOW()
		);
	`); err != nil {
		return fmt.Errorf("create video_urls table: %w", err)
	}
	log.Println("Table 'video_urls' ready")

	// Table: image_urls
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS image_urls (
			image_url TEXT PRIMARY KEY,
			response_id UUID REFERENCES search_response(response_id),
			is_citation BOOLEAN,
			seq_no INT,
			created_at TIMESTAMP DEFAULT NOW()
		);
	`); err != nil {
		return fmt.Errorf("create image_urls table: %w", err)
	}
	log.Println("Table 'image_urls' ready")

	// Table: web_metadata
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS web_metadata (
			url TEXT PRIMARY KEY,
			ai_overview TEXT,
			snippet_description TEXT,
			title VARCHAR(512),
			favicon TEXT,
			source VARCHAR(255),
			web_source VARCHAR(255),
			created_at TIMESTAMP DEFAULT NOW()
		);
	`); err != nil {
		return fmt.Errorf("create web_metadata table: %w", err)
	}
	log.Println("Table 'web_metadata' ready")

	// Table: image_metadata
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS image_metadata (
			url TEXT PRIMARY KEY,
			url_image TEXT,
			ai_overview TEXT,
			snippet_description TEXT,
			title VARCHAR(512),
			favicon TEXT,
			source VARCHAR(255),
			web_source VARCHAR(255),
			created_at TIMESTAMP DEFAULT NOW()
		);
	`); err != nil {
		return fmt.Errorf("create image_metadata table: %w", err)
	}
	log.Println("Table 'image_metadata' ready")

	// Table: video_metadata
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS video_metadata (
			video_id UUID PRIMARY KEY,
			video_url TEXT,
			title VARCHAR(512),
			thumbnail TEXT,
			publish_date DATE,
			likes BIGINT,
			views BIGINT,
			description TEXT,
			created_at TIMESTAMP DEFAULT NOW()
		);
	`); err != nil {
		return fmt.Errorf("create video_metadata table: %w", err)
	}
	log.Println("Table 'video_metadata' ready")

	return nil
}

