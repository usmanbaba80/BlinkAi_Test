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

// Config holds all application configuration
type Config struct {
	Port              string
	DatabaseURL       string
	DBHost            string
	DBPort            string
	DBUser            string
	DBPassword        string
	DBName            string
	ScrapingDogAPIKey string
}

// Load loads configuration from environment variables
func Load() *Config {
	// Load .env file if present
	_ = godotenv.Load()

	cfg := &Config{
		Port:              getEnv("PORT", "8000"),
		DatabaseURL:       os.Getenv("DATABASE_URL"),
		DBHost:            os.Getenv("DB_HOST"),
		DBPort:            getEnv("DB_PORT", "5432"),
		DBUser:            os.Getenv("DB_USER"),
		DBPassword:        os.Getenv("DB_PASSWORD"),
		DBName:            os.Getenv("DB_NAME"),
		ScrapingDogAPIKey: os.Getenv("SCRAPINGDOG_API_KEY"),
	}

	// Validate required configuration
	if cfg.ScrapingDogAPIKey == "" {
		log.Println("Warning: SCRAPINGDOG_API_KEY is not set")
	}

	return cfg
}

// getEnv gets an environment variable with a fallback default value
func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

// GetDSN returns the database connection string
func (c *Config) GetDSN() string {
	if c.DatabaseURL != "" {
		return c.DatabaseURL
	}

	if c.DBHost != "" && c.DBUser != "" && c.DBName != "" {
		return "host=" + c.DBHost + " port=" + c.DBPort + " user=" + c.DBUser +
			" password=" + c.DBPassword + " dbname=" + c.DBName + " sslmode=disable"
	}

	return ""
}
