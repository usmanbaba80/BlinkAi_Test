package main

import (
    "database/sql"
    "encoding/json"
    "io"
    "log"
    "math/rand"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"

    "github.com/joho/godotenv"
    "github.com/labstack/echo/v4"
    "github.com/labstack/echo/v4/middleware"
    _ "github.com/lib/pq"
)

var db *sql.DB

// initDB connects to PostgreSQL using env vars and ensures trends_now table exists
func initDB() error {
    dsn := os.Getenv("DATABASE_URL")
    if dsn == "" {
        // fallback individual params
        host := os.Getenv("DB_HOST")
        port := os.Getenv("DB_PORT")
        user := os.Getenv("DB_USER")
        pass := os.Getenv("DB_PASSWORD")
        name := os.Getenv("DB_NAME")
        if port == "" {
            port = "5432"
        }
        if host != "" && user != "" && name != "" {
            dsn = "host=" + host + " port=" + port + " user=" + user + " password=" + pass + " dbname=" + name + " sslmode=disable"
        }
    }
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
    db = conn
    log.Println("Database connected successfully!")

    // Ensure trends_now table exists
    _, err = db.Exec(`CREATE TABLE IF NOT EXISTS trends_now (
        id SERIAL PRIMARY KEY,
        trends_data JSONB NOT NULL,
        location VARCHAR(6) NOT NULL,
        created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
    );`)
    if err != nil {
        return err
    }
    log.Println("Table 'trends_now' ready")

    // Ensure query_results table exists
    _, err = db.Exec(`CREATE TABLE IF NOT EXISTS query_results (
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

// saveTrendsNow stores top trends JSON and geo into trends_now
func saveTrendsNow(trendsJSON []byte, location string) error {
    if db == nil {
        log.Println("DB is nil, cannot save trends")
        return nil
    }
    log.Printf("Saving trends to database for location: %s", location)
    _, err := db.Exec(`INSERT INTO trends_now (trends_data, location) VALUES ($1, $2)`, string(trendsJSON), location)
    if err != nil {
        log.Printf("Failed to insert into database: %v", err)
        return err
    }
    log.Printf("Successfully saved %d bytes of trends data to database", len(trendsJSON))
    return nil
}

// getRandomTrendsFromDB fetches the most recent trends for a location and returns 3 random items
func getRandomTrendsFromDB(location string) ([]interface{}, error) {
    if db == nil {
        return nil, nil
    }
    
    log.Printf("Checking database for existing trends for location: %s", location)
    
    var trendsData string
    err := db.QueryRow(`
        SELECT trends_data 
        FROM trends_now 
        WHERE location = $1 
        ORDER BY time_stamp DESC 
        LIMIT 1
    `, location).Scan(&trendsData)
    
    if err != nil {
        if err == sql.ErrNoRows {
            log.Printf("No existing trends found in database for location: %s", location)
            return nil, nil
        }
        log.Printf("Database query error: %v", err)
        return nil, err
    }
    
    log.Printf("Found existing trends in database for location: %s", location)
    
    // Parse the JSON array
    var allTrends []interface{}
    if err := json.Unmarshal([]byte(trendsData), &allTrends); err != nil {
        log.Printf("Failed to unmarshal trends data: %v", err)
        return nil, err
    }
    
    log.Printf("Total trends in database: %d", len(allTrends))
    
    // Return 3 random items
    if len(allTrends) <= 3 {
        return allTrends, nil
    }
    
    // Simple random selection without replacement
    rand.Seed(time.Now().UnixNano())
    
    selected := make([]interface{}, 0, 3)
    indices := rand.Perm(len(allTrends))[:3]
    
    for _, idx := range indices {
        selected = append(selected, allTrends[idx])
    }
    
    log.Printf("Returning 3 random trends from database")
    return selected, nil
}

// getCachedImageResults checks if we have cached results for a query and location
// Returns the images_results array if found, nil if not found
func getCachedImageResults(query string, location string) ([]interface{}, error) {
    if db == nil {
        return nil, nil
    }
    
    log.Printf("Checking database for cached image results - query: %s, location: %s", query, location)
    
    var resultsData string
    err := db.QueryRow(`
        SELECT results 
        FROM query_results 
        WHERE query = $1 AND location = $2 
        ORDER BY timestamp DESC 
        LIMIT 1
    `, query, location).Scan(&resultsData)
    
    if err != nil {
        if err == sql.ErrNoRows {
            log.Printf("No cached results found for query: %s, location: %s", query, location)
            return nil, nil
        }
        log.Printf("Database query error: %v", err)
        return nil, err
    }
    
    log.Printf("Found cached results in database")
    
    // Parse the full response JSON to extract images_results
    var fullResponse map[string]interface{}
    if err := json.Unmarshal([]byte(resultsData), &fullResponse); err != nil {
        log.Printf("Failed to unmarshal cached results: %v", err)
        return nil, err
    }
    
    // Extract images_results array
    if imagesResults, ok := fullResponse["images_results"].([]interface{}); ok {
        log.Printf("Found %d cached image results", len(imagesResults))
        return imagesResults, nil
    }
    
    log.Printf("No images_results field found in cached data")
    return nil, nil
}

// saveImageResults stores the full API response to database in the background
func saveImageResults(query string, searchType string, location string, fullResponse []byte) {
    go func() {
        if db == nil {
            log.Println("DB is nil, cannot save image results")
            return
        }
        
        log.Printf("Saving image results to database - query: %s, location: %s", query, location)
        _, err := db.Exec(`
            INSERT INTO query_results (query, searchType, location, results) 
            VALUES ($1, $2, $3, $4)
        `, query, searchType, location, string(fullResponse))
        
        if err != nil {
            log.Printf("Failed to save image results to database: %v", err)
            return
        }
        log.Printf("Successfully saved image results to database")
    }()
}

// paginateResults returns a slice of results based on start and end indices
func paginateResults(results []interface{}, start int, end int) []interface{} {
    total := len(results)
    
    // Validate and adjust start
    if start < 0 {
        start = 0
    }
    if start >= total {
        return []interface{}{}
    }
    
    // Validate and adjust end
    if end <= 0 || end > total {
        end = total
    }
    if end <= start {
        return []interface{}{}
    }
    
    return results[start:end]
}


// buildTrendsURL constructs the ScrapingDog Google Trends API URL
func buildTrendsURL(params map[string]string) string {
	var sb strings.Builder
	sb.WriteString("https://api.scrapingdog.com/google_trends/trending_now?")

	// api_key (from env)
	apiKey := os.Getenv("SCRAPINGDOG_API_KEY")
	sb.WriteString("api_key=")
	sb.WriteString(apiKey)

	// pass-through params
	for k, v := range params {
		if v == "" {
			continue
		}
		// avoid overriding required keys
		if k == "api_key" {
			continue
		}
		sb.WriteString("&")
		sb.WriteString(urlQueryEscape(k))
		sb.WriteString("=")
		sb.WriteString(urlQueryEscape(v))
	}

	return sb.String()
}

// buildImagesURL constructs the ScrapingDog Google Images API URL
func buildImagesURL(params map[string]string) string {
	var sb strings.Builder
	sb.WriteString("https://api.scrapingdog.com/google_images/?")

	// api_key (from env)
	apiKey := os.Getenv("SCRAPINGDOG_API_KEY")
	sb.WriteString("api_key=")
	sb.WriteString(apiKey)

	// pass-through params
	for k, v := range params {
		if v == "" {
			continue
		}
		// avoid overriding required keys
		if k == "api_key" {
			continue
		}
		sb.WriteString("&")
		sb.WriteString(urlQueryEscape(k))
		sb.WriteString("=")
		sb.WriteString(urlQueryEscape(v))
	}

	return sb.String()
}

// urlQueryEscape safely escapes query values
func urlQueryEscape(s string) string {
	return url.QueryEscape(strings.ReplaceAll(s, "\n", ""))
}

func main() {
	// Load environment variables from .env if present
	_ = godotenv.Load()

    // Initialize database connection (using env vars)
    if err := initDB(); err != nil {
        log.Printf("warning: db init failed: %v", err)
    }
    defer func() {
        if db != nil {
            _ = db.Close()
        }
    }()

    e := echo.New()
	e.HideBanner = true
	e.Use(middleware.Recover())
	e.Use(middleware.Logger())
    e.Use(middleware.CORS())

	// Health check
	e.GET("/health", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})


    // GET /trends?geo=US
	e.GET("/trends", func(c echo.Context) error {
		// Collect pass-through params for trends
		passParams := map[string]string{}
		for _, k := range []string{"geo", "query", "v"} {
			if val := c.QueryParam(k); val != "" {
				passParams[k] = val
			}
		}

		// Default to US if no geo specified
		if passParams["geo"] == "" {
			passParams["geo"] = "US"
		}

		location := passParams["geo"]

		// Step 1: Check if data exists in database
		randomTrends, err := getRandomTrendsFromDB(location)
		if err != nil {
			log.Printf("Error fetching from database: %v", err)
		}

		// Step 2: If data exists, return 3 random items
		if randomTrends != nil && len(randomTrends) > 0 {
			log.Printf("Returning %d random trends from database", len(randomTrends))
			return c.JSON(http.StatusOK, map[string]interface{}{
				"source":           "database",
				"location":         location,
				"count":            len(randomTrends),
				"trending_searches": randomTrends,
			})
		}

		// Step 3: No data in DB, call ScrapingDog API
		log.Println("No data in database, fetching from ScrapingDog API")
		
		apiKey := os.Getenv("SCRAPINGDOG_API_KEY")
		if apiKey == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "SCRAPINGDOG_API_KEY is not set"})
		}

		requestURL := buildTrendsURL(passParams)

        resp, err := http.Get(requestURL)
		if err != nil {
			log.Printf("scrapingdog trends request failed: %v", err)
			return c.JSON(http.StatusBadGateway, map[string]string{"error": "failed to reach trends service"})
		}
		defer resp.Body.Close()

        // Read body
        bodyBytes, err := io.ReadAll(resp.Body)
        if err != nil {
            log.Printf("error reading response: %v", err)
            return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to read response"})
        }

        // Parse JSON, extract top 10, save to DB, and return 3 random
        var parsed map[string]interface{}
        if err := json.Unmarshal(bodyBytes, &parsed); err == nil {
            log.Printf("Successfully parsed response JSON")
            // extract top 10 trending_searches if present
            if raw, ok := parsed["trending_searches"].([]interface{}); ok {
                log.Printf("Found %d trending searches", len(raw))
                top := raw
                if len(raw) > 10 {
                    top = raw[:10]
                    log.Printf("Limiting to top 10 results")
                }
                
                // Marshal top 10 back to JSON and save to DB
                topJSON, mErr := json.Marshal(top)
                if mErr == nil {
                    log.Printf("Marshaled top %d trends to JSON", len(top))
                    // Save to DB if available
                    if db != nil {
                        if saveErr := saveTrendsNow(topJSON, location); saveErr != nil {
                            log.Printf("save error: %v", saveErr)
                        }
                    } else {
                        log.Println("Database connection is nil, skipping save")
                    }
                }
                
                // Return 3 random items from the top 10
                rand.Seed(time.Now().UnixNano())
                selected := make([]interface{}, 0, 3)
                
                if len(top) <= 3 {
                    selected = top
                } else {
                    indices := rand.Perm(len(top))[:3]
                    for _, idx := range indices {
                        selected = append(selected, top[idx])
                    }
                }
                
                log.Printf("Returning %d random trends from API response", len(selected))
                return c.JSON(http.StatusOK, map[string]interface{}{
                    "source":           "api",
                    "location":         location,
                    "count":            len(selected),
                    "trending_searches": selected,
                })
            } else {
                log.Println("No 'trending_searches' field found in response")
            }
        } else {
            log.Printf("Failed to parse response JSON: %v", err)
        }

        // Fallback: return error
        return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to process trends data"})
	})

	// GET /images?query=mobiles&start=0&end=10&country=us&page=0
	e.GET("/images", func(c echo.Context) error {
		// Get query parameter (required)
		query := c.QueryParam("query")
		if query == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "missing required query parameter: query"})
		}

		// Get pagination parameters (start and end)
		start := 0
		end := 10 // default to 10 results
		
		if startParam := c.QueryParam("start"); startParam != "" {
			if val, err := strconv.Atoi(startParam); err == nil {
				start = val
			}
		}
		
		if endParam := c.QueryParam("end"); endParam != "" {
			if val, err := strconv.Atoi(endParam); err == nil {
				end = val
			}
		}

		// Get location/country parameter
		location := c.QueryParam("country")
		if location == "" {
			location = "us"
		}

		log.Printf("Image search request - query: %s, location: %s, start: %d, end: %d", 
			query, location, start, end)

		// Step 1: Check database for cached results
		cachedResults, err := getCachedImageResults(query, location)
		if err != nil {
			log.Printf("Error checking cache: %v", err)
		}

		// Step 2: If cached results found, return paginated data
		if cachedResults != nil && len(cachedResults) > 0 {
			log.Printf("Returning paginated results from cache (total: %d)", len(cachedResults))
			paginatedResults := paginateResults(cachedResults, start, end)
			
			return c.JSON(http.StatusOK, map[string]interface{}{
				"source":         "cache",
				"query":          query,
				"location":       location,
				"total_results":  len(cachedResults),
				"start":          start,
				"end":            end,
				"returned_count": len(paginatedResults),
				"images_results": paginatedResults,
			})
		}

		// Step 3: No cached data, call ScrapingDog API
		log.Printf("No cached results found, calling ScrapingDog API")
		
		apiKey := os.Getenv("SCRAPINGDOG_API_KEY")
		if apiKey == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "SCRAPINGDOG_API_KEY is not set"})
		}

		// Collect pass-through params for API
		passParams := map[string]string{
			"query": query,
		}

		// Optional parameters with defaults
		if results := c.QueryParam("results"); results != "" {
			passParams["results"] = results
		} else {
			passParams["results"] = "100"
		}

		passParams["country"] = location

		// Optional page parameter
		if page := c.QueryParam("page"); page != "" {
			passParams["page"] = page
		}

		requestURL := buildImagesURL(passParams)

		resp, err := http.Get(requestURL)
		if err != nil {
			log.Printf("scrapingdog images request failed: %v", err)
			return c.JSON(http.StatusBadGateway, map[string]string{"error": "failed to reach images service"})
		}
		defer resp.Body.Close()

		// Read body
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("error reading response: %v", err)
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": "failed to read response"})
		}

		log.Printf("Successfully fetched images from API, size: %d bytes", len(bodyBytes))

		// Step 4: Parse response, extract images_results, and save to DB in background
		var parsed map[string]interface{}
		if err := json.Unmarshal(bodyBytes, &parsed); err == nil {
			// Save full response to database in background
			searchType := "image_search"
			if st, ok := parsed["search_type"].(string); ok {
				searchType = st
			}
			saveImageResults(query, searchType, location, bodyBytes)

			// Extract images_results for pagination
			if imagesResults, ok := parsed["images_results"].([]interface{}); ok {
				log.Printf("Found %d images in API response", len(imagesResults))
				paginatedResults := paginateResults(imagesResults, start, end)
				
				return c.JSON(http.StatusOK, map[string]interface{}{
					"source":         "api",
					"query":          query,
					"location":       location,
					"total_results":  len(imagesResults),
					"start":          start,
					"end":            end,
					"returned_count": len(paginatedResults),
					"images_results": paginatedResults,
				})
			}
		}

		// Fallback: if we can't parse or extract images_results, return error
		log.Printf("Failed to parse API response or extract images_results")
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "failed to process images data from API",
		})
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("Starting server on port %s", port)
	if err := e.Start(":" + port); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}


