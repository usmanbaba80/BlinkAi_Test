package handlers

import (
	"net/http"

	"github.com/InvicttusGIT/BlinkAi_Merged/internal/models"
	"github.com/InvicttusGIT/BlinkAi_Merged/internal/services"
	"github.com/labstack/echo/v4"
)

// TrendsHandler handles trends-related requests
type TrendsHandler struct {
	service *services.TrendsService
}

// NewTrendsHandler creates a new TrendsHandler
func NewTrendsHandler(service *services.TrendsService) *TrendsHandler {
	return &TrendsHandler{service: service}
}

// GetTrends handles GET /trends requests
func (h *TrendsHandler) GetTrends(c echo.Context) error {
	// Collect pass-through params
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

	// Get random trends from service
	trends, source, err := h.service.GetRandomTrends(location, passParams)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:      "failed to process trends data",
			StatusCode: http.StatusInternalServerError,
			Success:    false,
		})
	}

	if trends == nil {
		return c.JSON(http.StatusInternalServerError, models.ErrorResponse{
			Error:      "failed to process trends data",
			StatusCode: http.StatusInternalServerError,
			Success:    false,
		})
	}

	return c.JSON(http.StatusOK, models.TrendResponse{
		Source:           source,
		Location:         location,
		Count:            len(trends),
		TrendingSearches: trends,
		StatusCode:       http.StatusOK,
		Success:          true,
	})
}

