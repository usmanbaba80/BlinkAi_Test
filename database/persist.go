package database

import (
	"context"
	"fmt"
	"time"

	model "github.com/InvicttusGIT/BlinkAi_SearchEngineApps_Productivity_Backend/models"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CreateSearchResponse inserts a new row into search_response and returns it
func CreateSearchResponse(ctx context.Context, pool *pgxpool.Pool, searchID uuid.UUID, aiSummary *string) (*model.SearchResponse, error) {
	query := `
		INSERT INTO search_response (response_id, search_id, ai_summary)
		VALUES (gen_random_uuid(), $1, $2)
		RETURNING response_id, search_id, ai_summary
	`
	var out model.SearchResponse
	if err := pool.QueryRow(ctx, query, searchID, aiSummary).Scan(&out.ResponseID, &out.SearchID, &out.AISummary); err != nil {
		return nil, fmt.Errorf("insert search_response: %w", err)
	}
	return &out, nil
}

// InsertVideoURL inserts into video_urls and returns the generated video_id
func InsertVideoURL(ctx context.Context, pool *pgxpool.Pool, responseID uuid.UUID, isCitation bool, seqNo *int) (uuid.UUID, error) {
	query := `
		INSERT INTO video_urls (video_id, response_id, is_citation, seq_no)
		VALUES (gen_random_uuid(), $1, $2, $3)
		RETURNING video_id
	`
	var videoID uuid.UUID
	if err := pool.QueryRow(ctx, query, responseID, isCitation, seqNo).Scan(&videoID); err != nil {
		return uuid.Nil, fmt.Errorf("insert video_urls: %w", err)
	}
	return videoID, nil
}

// InsertVideoMetadata inserts into video_metadata (expects a pre-existing video_id from video_urls)
func InsertVideoMetadata(ctx context.Context, pool *pgxpool.Pool, videoID uuid.UUID, m model.VideoMetadata) error {
	query := `
        INSERT INTO video_metadata (video_id, video_url, title, thumbnail, publish_date, likes, views, description)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (video_id) DO UPDATE SET
            video_url = EXCLUDED.video_url,
            title = EXCLUDED.title,
            thumbnail = EXCLUDED.thumbnail,
            publish_date = EXCLUDED.publish_date,
            likes = EXCLUDED.likes,
            views = EXCLUDED.views,
            description = EXCLUDED.description
    `
	if _, err := pool.Exec(ctx, query, videoID, m.VideoURL, m.Title, m.Thumbnail, m.PublishDate, m.Likes, m.Views, m.Description); err != nil {
		return fmt.Errorf("insert video_metadata: %w", err)
	}
	return nil
}

// InsertWebURL inserts into web_urls
func InsertWebURL(ctx context.Context, pool *pgxpool.Pool, responseID uuid.UUID, webURL string, isCitation bool, seqNo *int) error {
	query := `
        INSERT INTO web_urls (web_url, response_id, is_citation, seq_no)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (web_url) DO UPDATE SET
            response_id = EXCLUDED.response_id,
            is_citation = EXCLUDED.is_citation,
            seq_no = EXCLUDED.seq_no
    `
	if _, err := pool.Exec(ctx, query, webURL, responseID, isCitation, seqNo); err != nil {
		return fmt.Errorf("insert web_urls: %w", err)
	}
	return nil
}

// InsertWebMetadata inserts into web_metadata
func InsertWebMetadata(ctx context.Context, pool *pgxpool.Pool, meta model.WebItem) error {
	query := `
        INSERT INTO web_metadata (url, ai_overview, snippet_description, title, favicon, source)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (url) DO UPDATE SET
            ai_overview = EXCLUDED.ai_overview,
            snippet_description = EXCLUDED.snippet_description,
            title = EXCLUDED.title,
            favicon = EXCLUDED.favicon,
            source = EXCLUDED.source
    `
	// Map WebItem to metadata schema
	var snippet *string
	if meta.Snippet != "" {
		t := meta.Snippet
		snippet = &t
	}
	var ai *string
	if meta.AIOverview != "" {
		t := meta.AIOverview
		ai = &t
	}
	var title *string
	if meta.Title != "" {
		t := meta.Title
		title = &t
	}
	var favicon *string
	if meta.Favicon != "" {
		t := meta.Favicon
		favicon = &t
	}
	var source *string
	if meta.Source != "" {
		t := meta.Source
		source = &t
	}
	if _, err := pool.Exec(ctx, query, meta.URL, ai, snippet, title, favicon, source); err != nil {
		return fmt.Errorf("insert web_metadata: %w", err)
	}
	return nil
}

// InsertImageURL inserts into image_urls
func InsertImageURL(ctx context.Context, pool *pgxpool.Pool, responseID uuid.UUID, imageURL string, isCitation bool, seqNo *int) error {
	query := `
        INSERT INTO image_urls (image_url, response_id, is_citation, seq_no)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (image_url) DO UPDATE SET
            response_id = EXCLUDED.response_id,
            is_citation = EXCLUDED.is_citation,
            seq_no = EXCLUDED.seq_no
    `
	if _, err := pool.Exec(ctx, query, imageURL, responseID, isCitation, seqNo); err != nil {
		return fmt.Errorf("insert image_urls: %w", err)
	}
	return nil
}

// InsertImageMetadata inserts into image_metadata
func InsertImageMetadata(ctx context.Context, pool *pgxpool.Pool, meta model.ImageItem) error {
	query := `
        INSERT INTO image_metadata (url, url_image, ai_overview, snippet_description, title, favicon, source)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (url) DO UPDATE SET
            url_image = EXCLUDED.url_image,
            ai_overview = EXCLUDED.ai_overview,
            snippet_description = EXCLUDED.snippet_description,
            title = EXCLUDED.title,
            favicon = EXCLUDED.favicon,
            source = EXCLUDED.source
    `
	var urlImage *string
	if meta.ImageURL != "" {
		t := meta.ImageURL
		urlImage = &t
	}
	var ai *string
	if meta.AIOverview != "" {
		t := meta.AIOverview
		ai = &t
	}
	var snippet *string
	if meta.Snippet != "" {
		t := meta.Snippet
		snippet = &t
	}
	var title *string
	if meta.Title != "" {
		t := meta.Title
		title = &t
	}
	var favicon *string
	if meta.Favicon != "" {
		t := meta.Favicon
		favicon = &t
	}
	var source *string
	if meta.Source != "" {
		t := meta.Source
		source = &t
	}
	if _, err := pool.Exec(ctx, query, meta.OriginURL, urlImage, ai, snippet, title, favicon, source); err != nil {
		return fmt.Errorf("insert image_metadata: %w", err)
	}
	return nil
}

// Helper to convert string date (YYYY-MM-DD) to *time.Time if needed
func ParseDatePtr(s string) *time.Time {
	if s == "" {
		return nil
	}
	if t, err := time.Parse("2006-01-02", s); err == nil {
		return &t
	}
	return nil
}
