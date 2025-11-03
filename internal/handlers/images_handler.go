package handlers

import (
	"log"
	"net/http"
	"strconv"

	"github.com/InvicttusGIT/BlinkAi_Merged/internal/models"
	"github.com/InvicttusGIT/BlinkAi_Merged/internal/services"
	"github.com/labstack/echo/v4"
)

// ImagesHandler handles image search requests
type ImagesHandler struct {
	service *services.ImagesService
}

// NewImagesHandler creates a new ImagesHandler
func NewImagesHandler(service *services.ImagesService) *ImagesHandler {
	return &ImagesHandler{service: service}
}

// SearchImages handles GET /images requests
func (h *ImagesHandler) SearchImages(c echo.Context) error {
	// Get query parameter (required)
	query := c.QueryParam("query")
	if query == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "missing required query parameter: query",
		})
	}

	// Get pagination parameters
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

	// Build params for API call
	passParams := map[string]string{
		"query":   query,
		"country": location,
	}

	// Optional parameters with defaults
	if results := c.QueryParam("results"); results != "" {
		passParams["results"] = results
	} else {
		passParams["results"] = "100"
	}

	// Optional page parameter
	if page := c.QueryParam("page"); page != "" {
		passParams["page"] = page
	}

	// Call service
	imagesResults, totalResults, source, err := h.service.SearchImages(query, location, start, end, passParams)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"error": "failed to process images data from API",
		})
	}

	return c.JSON(http.StatusOK, models.ImageResponse{
		Source:        source,
		Query:         query,
		Location:      location,
		TotalResults:  totalResults,
		Start:         start,
		End:           end,
		ReturnedCount: len(imagesResults),
		ImagesResults: imagesResults,
	})
}

