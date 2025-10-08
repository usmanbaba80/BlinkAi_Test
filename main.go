package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/InvicttusGIT/BlinkAi_SearchEngineApps_Productivity_Backend/config"
	"github.com/InvicttusGIT/BlinkAi_SearchEngineApps_Productivity_Backend/handlers"
	"github.com/InvicttusGIT/BlinkAi_SearchEngineApps_Productivity_Backend/modules"
	"github.com/go-playground/validator/v10"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

func main() {
	// Load .env if present (safe in all environments)
	config.LoadEnv()

	// Create a bounded context for connect + ping
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := config.Connect(ctx)
	if err != nil {
		log.Printf("Database connection failed: %v", err)
		fmt.Println("Database connection: FAILED")
		return
	}
	defer pool.Close()

	fmt.Println("Database connection: SUCCESS")

	// Load system prompts from database
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

	// Routes
	e.POST("/search", handlers.SearchHandler(pool))

	// Start server
	port := ":8000"
	fmt.Printf("Server starting on port %s\n", port)
	// Start TCP streaming server early to allow clients to register before HTTP stream begins
	modules.EnsureTCPServer(":9000")
	log.Fatal(e.Start(port))
}

// CustomValidator is a custom validator for Echo
type CustomValidator struct {
	validator *validator.Validate
}

// Validate validates the struct
func (cv *CustomValidator) Validate(i interface{}) error {
	return cv.validator.Struct(i)
}
