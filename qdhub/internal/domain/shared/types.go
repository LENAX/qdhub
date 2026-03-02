// Package shared provides shared types and utilities for the domain layer.
package shared

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// ==================== 基础类型 ====================

// ID represents a unique identifier for entities.
type ID string

// NewID generates a new unique ID.
func NewID() ID {
	return ID(uuid.New().String())
}

// String returns the string representation of the ID.
func (id ID) String() string {
	return string(id)
}

// IsEmpty checks if the ID is empty.
func (id ID) IsEmpty() bool {
	return id == ""
}

// Timestamp represents a point in time.
type Timestamp time.Time

// Now returns the current timestamp.
func Now() Timestamp {
	return Timestamp(time.Now())
}

// ToTime converts Timestamp to time.Time.
func (t Timestamp) ToTime() time.Time {
	return time.Time(t)
}

// IsZero checks if the timestamp is zero.
func (t Timestamp) IsZero() bool {
	return time.Time(t).IsZero()
}

// MarshalJSON implements json.Marshaler interface.
func (t Timestamp) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(t))
}

// UnmarshalJSON implements json.Unmarshaler interface.
func (t *Timestamp) UnmarshalJSON(data []byte) error {
	var tm time.Time
	if err := json.Unmarshal(data, &tm); err != nil {
		return err
	}
	*t = Timestamp(tm)
	return nil
}

// ==================== 状态枚举 ====================

// Status represents the status of an entity.
type Status string

const (
	StatusActive   Status = "active"
	StatusInactive Status = "inactive"
	StatusPending  Status = "pending"
	StatusFailed   Status = "failed"
	StatusSuccess  Status = "success"
)

// String returns the string representation of the status.
func (s Status) String() string {
	return string(s)
}

// IsValid checks if the status is valid.
func (s Status) IsValid() bool {
	switch s {
	case StatusActive, StatusInactive, StatusPending, StatusFailed, StatusSuccess:
		return true
	default:
		return false
	}
}

// ==================== 通用错误 ====================

// DomainError represents a domain-level error.
type DomainError struct {
	Code    string
	Message string
	Cause   error
}

func (e *DomainError) Error() string {
	if e.Cause != nil {
		return e.Message + ": " + e.Cause.Error()
	}
	return e.Message
}

// NewDomainError creates a new domain error.
func NewDomainError(code, message string, cause error) *DomainError {
	return &DomainError{
		Code:    code,
		Message: message,
		Cause:   cause,
	}
}

// Common domain error codes
const (
	ErrCodeValidation   = "VALIDATION_ERROR"
	ErrCodeNotFound     = "NOT_FOUND"
	ErrCodeConflict     = "CONFLICT"
	ErrCodeInvalidState = "INVALID_STATE"
)

// IsNotFoundError checks if the error is a not found error.
func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if domainErr, ok := err.(*DomainError); ok {
		return domainErr.Code == ErrCodeNotFound
	}
	return false
}
