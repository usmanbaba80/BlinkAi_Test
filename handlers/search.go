package handlers

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/InvicttusGIT/BlinkAi_SearchEngineApps_Productivity_Backend/config"
	"github.com/InvicttusGIT/BlinkAi_SearchEngineApps_Productivity_Backend/database"
	"github.com/InvicttusGIT/BlinkAi_SearchEngineApps_Productivity_Backend/models"
	"github.com/InvicttusGIT/BlinkAi_SearchEngineApps_Productivity_Backend/modules"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
)

// SearchHandler handles the search endpoint
func SearchHandler(pool *pgxpool.Pool) echo.HandlerFunc {
	return func(c echo.Context) error {
		var req models.SearchRequest

		// Bind the request body to the SearchRequest struct
		if err := c.Bind(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"message": "Invalid request format",
				"error":   err.Error(),
			})
		}

		// Validate the request
		if err := c.Validate(&req); err != nil {
			return c.JSON(http.StatusBadRequest, map[string]interface{}{
				"success": false,
				"message": "Validation failed",
				"error":   err.Error(),
			})
		}
		// Validate user - get existing user or create new one
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		user, err := database.GetOrCreateUser(ctx, pool, req.UserID, req.DeviceID, req.PlatformName)
		if err != nil {
			// Check if it's a timeout error
			if ctx.Err() == context.DeadlineExceeded {
				return c.JSON(http.StatusRequestTimeout, map[string]interface{}{
					"success": false,
					"message": "User validation timeout - please try again",
					"error":   "Database operation timed out",
				})
			}

			return c.JSON(http.StatusInternalServerError, map[string]interface{}{
				"success": false,
				"message": "Failed to validate/create user",
				"error":   err.Error(),
			})
		}

		// Get the system prompt for this search type (in-memory, very fast)
		promptData, promptFound := config.SystemPrompts.GetPrompt(req.SearchType, req.PlatformName)

		// Store the search request in the database
		var systemPromptID string
		if promptFound {
			systemPromptID = promptData.ID.String()
		}

		searchRecord, err := database.CreateSearch(ctx, pool, req.UserID, systemPromptID, req.Keywords)
		if err != nil {
			// Log error but don't fail the request - search can still proceed
			fmt.Printf("Warning: Failed to store search in database: %v\n", err)
			searchRecord = nil // Set to nil so we can handle it in response
		} else {
			fmt.Printf("Search stored in database with ID: %s\n", searchRecord.SearchID)
		}
		// Build Perplexity messages (system + alternating user/assistant from keywords)
		sysPrompt := ""
		if promptFound {
			sysPrompt = promptData.PromptText
		}
		messages := modules.BuildMessages(sysPrompt, req.Keywords)

		if req.SearchType == "all" {
			fmt.Println("all api hit for perplexity")
			// Prepare stream to Perplexity for video
			pReq := models.PerplexityRequest{
				Model:    "sonar-pro",
				Messages: messages,
				Stream:   true,
				MediaResponse: map[string]any{
					"overrides": map[string]any{"return_videos": true, "return_images": true},
				},
				ReturnRelatedQuestions: true,
				//SearchDomainFilter:     []string{"youtube.com"},
				Temperature:      0.1,
				MaxSearchResults: 10,
			}
			// Background processing via module helper
			go modules.StartBackgroundPerplexity(context.Background(), modules.MustGetPerplexityKey(), pReq, 90*time.Second, searchRecord.SearchID.String()+"_ai_summary", searchRecord.SearchID.String()+"_list", searchRecord, pool, req.PlatformName, func(object, content string) {
				fmt.Printf("object=%s content_chunk=%q\n", object, content)
			})

		} else {
			fmt.Println("video api hit for perplexity")
			// Prepare stream to Perplexity for video
			pReq := models.PerplexityRequest{
				Model:    "sonar-pro",
				Messages: messages,
				Stream:   true,
				MediaResponse: map[string]any{
					"overrides": map[string]any{"return_videos": true, "return_images": true},
				},
				ReturnRelatedQuestions: true,
				SearchDomainFilter:     []string{"youtube.com"},
				Temperature:            0.2,
			}
			// Background processing via module helper
			go modules.StartBackgroundPerplexity(context.Background(), modules.MustGetPerplexityKey(), pReq, 90*time.Second, searchRecord.SearchID.String()+"_ai_summary", searchRecord.SearchID.String()+"_list", searchRecord, pool, req.PlatformName, func(object, content string) {
				fmt.Printf("object=%s content_chunk=%q\n", object, content)
			})
		}

		// Immediate ack to client
		ack := map[string]any{
			//"search_id":   searchRecord.SearchID.String(),
			"success":     true,
			"message":     "Your request is being processed",
			"user_id":     req.UserID,
			"search_type": req.SearchType,
			"keywords":    req.Keywords,
			"timestamp":   time.Now().Format(time.RFC3339),
		}
		if searchRecord != nil {
			ack["search_id_ai_summary"] = searchRecord.SearchID.String() + "_ai_summary"
			ack["search_id_list"] = searchRecord.SearchID.String() + "_list"

		}
		if promptFound {
			ack["system_prompt_id"] = promptData.ID.String()
		}

		// Print the received data to console
		fmt.Printf("Received Search Request (Goroutine: %p):\n", c.Request())
		fmt.Printf("Keywords: %v\n", req.Keywords)
		fmt.Printf("User ID: %s\n", req.UserID)
		fmt.Printf("Device ID: %s\n", req.DeviceID)
		fmt.Printf("Platform Name: %s\n", req.PlatformName)
		fmt.Printf("Search Type: %s\n", req.SearchType)
		fmt.Printf("User Status: %s (Created: %s)\n", user.ID, user.CreatedAt.Format("2006-01-02 15:04:05"))
		fmt.Printf("System Prompt Found: %t\n", promptFound)
		if promptFound {
			fmt.Printf("System Prompt ID: %s\n", promptData.ID)
			fmt.Printf("System Prompt Type: %s\n", promptData.SearchType)
			fmt.Printf("System Prompt: %s\n", promptData.PromptText)
		}
		fmt.Printf("Request Time: %s\n", time.Now().Format("15:04:05.000"))
		fmt.Printf("---\n")

		return c.JSON(http.StatusAccepted, ack)

	}
}

// messages builder moved to modules.BuildMessages
