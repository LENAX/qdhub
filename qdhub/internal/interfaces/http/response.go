// Package http provides HTTP API handlers for QDHub.
package http

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"qdhub/internal/domain/shared"
)

// Response represents a standard API response.
type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// PagedResponse represents a paginated API response.
type PagedResponse struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
	Total   int64       `json:"total"`
	Page    int         `json:"page"`
	Size    int         `json:"size"`
}

// Success sends a successful response with data.
func Success(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, Response{
		Code:    0,
		Message: "success",
		Data:    data,
	})
}

// SuccessWithMessage sends a successful response with a custom message.
func SuccessWithMessage(c *gin.Context, message string, data interface{}) {
	c.JSON(http.StatusOK, Response{
		Code:    0,
		Message: message,
		Data:    data,
	})
}

// Created sends a 201 Created response.
func Created(c *gin.Context, data interface{}) {
	c.JSON(http.StatusCreated, Response{
		Code:    0,
		Message: "created",
		Data:    data,
	})
}

// NoContent sends a 204 No Content response.
func NoContent(c *gin.Context) {
	c.Status(http.StatusNoContent)
}

// Paged sends a paginated response.
func Paged(c *gin.Context, data interface{}, total int64, page, size int) {
	c.JSON(http.StatusOK, PagedResponse{
		Code:    0,
		Message: "success",
		Data:    data,
		Total:   total,
		Page:    page,
		Size:    size,
	})
}

// Error sends an error response.
func Error(c *gin.Context, statusCode int, code int, message string) {
	c.JSON(statusCode, Response{
		Code:    code,
		Message: message,
	})
}

// BadRequest sends a 400 Bad Request response.
func BadRequest(c *gin.Context, message string) {
	Error(c, http.StatusBadRequest, 400, message)
}

// NotFound sends a 404 Not Found response.
func NotFound(c *gin.Context, message string) {
	Error(c, http.StatusNotFound, 404, message)
}

// Conflict sends a 409 Conflict response.
func Conflict(c *gin.Context, message string) {
	Error(c, http.StatusConflict, 409, message)
}

// InternalError sends a 500 Internal Server Error response.
func InternalError(c *gin.Context, message string) {
	Error(c, http.StatusInternalServerError, 500, message)
}

// HandleError handles domain errors and sends appropriate HTTP responses.
func HandleError(c *gin.Context, err error) {
	if err == nil {
		return
	}

	if domainErr, ok := err.(*shared.DomainError); ok {
		switch domainErr.Code {
		case shared.ErrCodeNotFound:
			NotFound(c, domainErr.Message)
		case shared.ErrCodeValidation:
			BadRequest(c, domainErr.Message)
		case shared.ErrCodeConflict:
			Conflict(c, domainErr.Message)
		case shared.ErrCodeInvalidState:
			BadRequest(c, domainErr.Message)
		default:
			InternalError(c, domainErr.Message)
		}
		return
	}

	InternalError(c, err.Error())
}
