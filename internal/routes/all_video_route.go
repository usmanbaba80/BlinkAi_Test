package routes

import (
	"github.com/InvicttusGIT/BlinkAi_Merged/internal/handlers"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
)

// Register wires all HTTP routes onto the provided Echo instance.
// This registers endpoints from all three projects:
// - Search endpoint from project 1
// - Trends endpoint from project 2
// - Images endpoint from project 2
func Register(
	e *echo.Echo,
	pool *pgxpool.Pool,
	healthHandler *handlers.HealthHandler,
	trendsHandler *handlers.TrendsHandler,
	imagesHandler *handlers.ImagesHandler,
) {
	// Health check endpoint
	e.GET("/health", healthHandler.Check)

	// Search endpoint (from project 1)
	e.POST("/search", handlers.SearchHandler(pool))

	// Trends endpoint (from project 2)
	e.GET("/trends", trendsHandler.GetTrends)

	// Images endpoint (from project 2)
	e.GET("/images", imagesHandler.SearchImages)
}

