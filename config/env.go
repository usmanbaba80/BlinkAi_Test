package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

// LoadEnv loads variables from a .env file if present. It is safe to call multiple times.
func LoadEnv() {
	// If already loaded (e.g., in production via real env vars), skip errors.
	if _, err := os.Stat(".env"); err == nil {
		if err := godotenv.Load(); err != nil {
			log.Printf("warning: failed to load .env: %v", err)
		}
	}
}
