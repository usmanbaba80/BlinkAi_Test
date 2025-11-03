package database

import (
	"database/sql"
	"log"

	_ "github.com/lib/pq"
)

// DB holds the database connection
var DB *sql.DB

// Connect establishes a connection to PostgreSQL
func Connect(dsn string) error {
	if dsn == "" {
		log.Println("No database credentials found. Set DATABASE_URL or DB_HOST/DB_PORT/DB_USER/DB_PASSWORD/DB_NAME")
		return nil
	}

	log.Printf("Connecting to database...")
	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}

	if err := conn.Ping(); err != nil {
		return err
	}

	DB = conn
	log.Println("Database connected successfully!")

	// Initialize tables
	if err := initTables(); err != nil {
		return err
	}

	return nil
}

// Close closes the database connection
func Close() error {
	if DB != nil {
		return DB.Close()
	}
	return nil
}

// initTables creates necessary database tables
func initTables() error {
	// Create trends_now table (only if not exists - preserve data)
	_, err := DB.Exec(`CREATE TABLE IF NOT EXISTS trends_now (
		id SERIAL PRIMARY KEY,
		trends_data JSONB NOT NULL,
		location VARCHAR(255) NOT NULL,
		timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
	);`)
	if err != nil {
		return err
	}
	log.Println("Table 'trends_now' ready")

	// Create query_results table (only if not exists - preserve data)
	_, err = DB.Exec(`CREATE TABLE IF NOT EXISTS query_results (
		idx SERIAL PRIMARY KEY,
		query TEXT,
		searchType VARCHAR(512),
		location VARCHAR(255) DEFAULT 'US',
		results JSONB,
		timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
	);`)
	if err != nil {
		return err
	}
	log.Println("Table 'query_results' ready")

	return nil
}

// GetDB returns the database connection
func GetDB() *sql.DB {
	return DB
}
