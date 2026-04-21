package metrics

import (
	"fmt"

	"qdhub/internal/domain/shared"
)

type FactorDirection string

const (
	FactorDirectionHigherBetter FactorDirection = "higher_better"
	FactorDirectionLowerBetter  FactorDirection = "lower_better"
)

type FactorSpec struct {
	Direction FactorDirection `json:"direction"`
}

func (s *FactorSpec) Validate() error {
	if s == nil {
		return shared.NewDomainError(shared.ErrCodeValidation, "factor_spec is required", nil)
	}
	if s.Direction == "" {
		s.Direction = FactorDirectionHigherBetter
	}
	switch s.Direction {
	case FactorDirectionHigherBetter, FactorDirectionLowerBetter:
		return nil
	default:
		return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("unsupported factor direction: %s", s.Direction), nil)
	}
}
