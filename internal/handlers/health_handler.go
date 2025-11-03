package handlers

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

// HealthHandler handles health check requests
type HealthHandler struct{}

// NewHealthHandler creates a new HealthHandler
func NewHealthHandler() *HealthHandler {
	return &HealthHandler{}
}

// Check returns the health status
func (h *HealthHandler) Check(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
}

