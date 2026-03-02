// Package metadata contains the metadata domain service implementations.
package metadata

import (
	"fmt"
	"strings"

	"qdhub/internal/domain/shared"
)

// ==================== MetadataValidator 实现 ====================

type metadataValidatorImpl struct{}

// NewMetadataValidator creates a new MetadataValidator.
func NewMetadataValidator() MetadataValidator {
	return &metadataValidatorImpl{}
}

// ValidateAPIMetadata validates API metadata completeness and correctness.
func (v *metadataValidatorImpl) ValidateAPIMetadata(apiMetadata *APIMetadata) error {
	if apiMetadata == nil {
		return shared.NewDomainError(shared.ErrCodeValidation, "API metadata cannot be nil", nil)
	}

	if apiMetadata.ID.IsEmpty() {
		return shared.NewDomainError(shared.ErrCodeValidation, "API metadata ID cannot be empty", nil)
	}

	if apiMetadata.DataSourceID.IsEmpty() {
		return shared.NewDomainError(shared.ErrCodeValidation, "data source ID cannot be empty", nil)
	}

	if strings.TrimSpace(apiMetadata.Name) == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "API name cannot be empty", nil)
	}

	if strings.TrimSpace(apiMetadata.Endpoint) == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "API endpoint cannot be empty", nil)
	}

	// Validate request parameters
	for i, param := range apiMetadata.RequestParams {
		if strings.TrimSpace(param.Name) == "" {
			return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("request parameter[%d] name cannot be empty", i), nil)
		}
		if strings.TrimSpace(param.Type) == "" {
			return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("request parameter[%d] type cannot be empty", i), nil)
		}
	}

	// Validate response fields
	if len(apiMetadata.ResponseFields) == 0 {
		return shared.NewDomainError(shared.ErrCodeValidation, "API must have at least one response field", nil)
	}

	for i, field := range apiMetadata.ResponseFields {
		if strings.TrimSpace(field.Name) == "" {
			return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("response field[%d] name cannot be empty", i), nil)
		}
		if strings.TrimSpace(field.Type) == "" {
			return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("response field[%d] type cannot be empty", i), nil)
		}
	}

	// Validate rate limit if set
	if apiMetadata.RateLimit != nil {
		if apiMetadata.RateLimit.RequestsPerMinute <= 0 {
			return shared.NewDomainError(shared.ErrCodeValidation, "requests per minute must be positive", nil)
		}
	}

	return nil
}

// ValidateDataSource validates data source configuration.
func (v *metadataValidatorImpl) ValidateDataSource(dataSource *DataSource) error {
	if dataSource == nil {
		return shared.NewDomainError(shared.ErrCodeValidation, "data source cannot be nil", nil)
	}

	if dataSource.ID.IsEmpty() {
		return shared.NewDomainError(shared.ErrCodeValidation, "data source ID cannot be empty", nil)
	}

	if strings.TrimSpace(dataSource.Name) == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "data source name cannot be empty", nil)
	}

	if strings.TrimSpace(dataSource.BaseURL) == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "base URL cannot be empty", nil)
	}

	// Validate URL format (simple check)
	if !strings.HasPrefix(dataSource.BaseURL, "http://") && !strings.HasPrefix(dataSource.BaseURL, "https://") {
		return shared.NewDomainError(shared.ErrCodeValidation, "base URL must start with http:// or https://", nil)
	}

	if !dataSource.Status.IsValid() {
		return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("invalid status: %s", dataSource.Status), nil)
	}

	return nil
}

// ValidateToken validates token configuration.
func (v *metadataValidatorImpl) ValidateToken(token *Token) error {
	if token == nil {
		return shared.NewDomainError(shared.ErrCodeValidation, "token cannot be nil", nil)
	}

	if token.ID.IsEmpty() {
		return shared.NewDomainError(shared.ErrCodeValidation, "token ID cannot be empty", nil)
	}

	if token.DataSourceID.IsEmpty() {
		return shared.NewDomainError(shared.ErrCodeValidation, "data source ID cannot be empty", nil)
	}

	if strings.TrimSpace(token.TokenValue) == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "token value cannot be empty", nil)
	}

	// Check if token is expired
	if token.IsExpired() {
		return shared.NewDomainError(shared.ErrCodeValidation, "token has expired", nil)
	}

	return nil
}
