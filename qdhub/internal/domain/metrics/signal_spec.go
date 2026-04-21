package metrics

import (
	"fmt"

	"qdhub/internal/domain/shared"
)

type SignalOutputKind string

const (
	SignalOutputBool SignalOutputKind = "bool"
	SignalOutputEnum SignalOutputKind = "enum_string"
)

type SignalSpec struct {
	OutputKind SignalOutputKind `json:"output_kind"`
}

func (s *SignalSpec) Validate() error {
	if s == nil {
		return shared.NewDomainError(shared.ErrCodeValidation, "signal_spec is required", nil)
	}
	if s.OutputKind == "" {
		s.OutputKind = SignalOutputBool
	}
	switch s.OutputKind {
	case SignalOutputBool, SignalOutputEnum:
		return nil
	default:
		return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("unsupported signal output_kind: %s", s.OutputKind), nil)
	}
}
