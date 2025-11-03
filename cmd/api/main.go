package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/InvicttusGIT/BlinkAi_Merged/internal/config"
	"github.com/InvicttusGIT/BlinkAi_Merged/internal/database"
	"github.com/InvicttusGIT/BlinkAi_Merged/internal/handlers"
	"github.com/InvicttusGIT/BlinkAi_Merged/internal/modules"
	"github.com/InvicttusGIT/BlinkAi_Merged/internal/repository"
	"github.com/InvicttusGIT/BlinkAi_Merged/internal/routes"
	"github.com/InvicttusGIT/BlinkAi_Merged/internal/services"
	"github.com/go-playground/validator/v10"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

func main() {
	// Load .env if present (safe in all environments)
	config.LoadEnv()

	// Load application configuration
	cfg := config.Load()

	// Create a bounded context for database connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Initialize database connection (pgxpool for search project)
	pool, err := config.Connect(ctx)
	if err != nil {
		log.Printf("Database connection failed: %v", err)
		fmt.Println("Database connection: FAILED")
		return
	}
	defer pool.Close()

	fmt.Println("Database connection: SUCCESS")

	// Initialize SQL database connection for trends/images (lib/pq)
	if err := database.Connect(cfg.GetDSN()); err != nil {
		log.Printf("warning: db init failed: %v", err)
	}
	defer func() {
		if err := database.Close(); err != nil {
			log.Printf("error closing database: %v", err)
		}
	}()

	// Load system prompts from database (for search functionality)
	if err := config.InitializePrompts(ctx, pool); err != nil {
		log.Printf("Failed to load system prompts: %v", err)
		fmt.Println("System prompts loading: FAILED")
		return
	}
	fmt.Println("System prompts loading: SUCCESS")

	// Initialize Echo server
	e := echo.New()

	// Validator
	e.Validator = &CustomValidator{validator: validator.New()}

	// Middleware
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(middleware.CORS())

	// Initialize repositories
	trendsRepo := repository.NewTrendsRepository(database.GetDB())
	imagesRepo := repository.NewImagesRepository(database.GetDB())

	// Initialize services
	trendsService := services.NewTrendsService(trendsRepo, cfg.ScrapingDogAPIKey)
	imagesService := services.NewImagesService(imagesRepo, cfg.ScrapingDogAPIKey)

	// Initialize handlers
	healthHandler := handlers.NewHealthHandler()
	trendsHandler := handlers.NewTrendsHandler(trendsService)
	imagesHandler := handlers.NewImagesHandler(imagesService)

	// Register all routes
	routes.Register(e, pool, healthHandler, trendsHandler, imagesHandler)

	// Start server
	port := ":" + cfg.Port
	fmt.Printf("Server starting on port %s\n", port)

	// Start TCP streaming server early to allow clients to register before HTTP stream begins
	modules.EnsureTCPServer(":9000")

	if err := e.Start(port); err != nil {
		log.Printf("Server failed to start: %v", err)
		return
	}
}

// CustomValidator is a custom validator for Echo
type CustomValidator struct {
	validator *validator.Validate
}

// Validate validates the struct
func (cv *CustomValidator) Validate(i interface{}) error {
	return cv.validator.Struct(i)
}

