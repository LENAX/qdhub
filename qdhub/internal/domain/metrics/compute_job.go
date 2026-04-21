package metrics

import (
	"fmt"
	"strings"
	"time"

	"qdhub/internal/domain/shared"
)

type ComputeJobType string

const (
	ComputeJobTypeFactorRecalculate ComputeJobType = "factor_recalculate"
	ComputeJobTypeSignalRecalculate ComputeJobType = "signal_recalculate"
	ComputeJobTypeUniverseMaterial  ComputeJobType = "universe_materialize"
)

type ComputeRangeType string

const (
	ComputeRangeIncremental ComputeRangeType = "incremental"
	ComputeRangeDateRange   ComputeRangeType = "date_range"
	ComputeRangeFullRebuild ComputeRangeType = "full_rebuild"
)

type ComputeJobStatus string

const (
	ComputeJobStatusPending   ComputeJobStatus = "pending"
	ComputeJobStatusQueued    ComputeJobStatus = "queued"
	ComputeJobStatusRunning   ComputeJobStatus = "running"
	ComputeJobStatusSucceeded ComputeJobStatus = "succeeded"
	ComputeJobStatusFailed    ComputeJobStatus = "failed"
)

type ComputeJob struct {
	ID            string           `json:"job_id"`
	JobType       ComputeJobType   `json:"job_type"`
	TargetIDs     []string         `json:"target_ids"`
	RangeType     ComputeRangeType `json:"range_type"`
	StartTime     string           `json:"start_time,omitempty"`
	EndTime       string           `json:"end_time,omitempty"`
	TriggerReason string           `json:"trigger_reason,omitempty"`
	Priority      int              `json:"priority,omitempty"`
	Status        ComputeJobStatus `json:"status"`
	ErrorMessage  string           `json:"error_message,omitempty"`
	ResultSummary string           `json:"result_summary,omitempty"`
	CreatedAt     time.Time        `json:"created_at"`
	StartedAt     *time.Time       `json:"started_at,omitempty"`
	FinishedAt    *time.Time       `json:"finished_at,omitempty"`
}

func NewComputeJob(jobType ComputeJobType, targetIDs []string, rangeType ComputeRangeType) *ComputeJob {
	return &ComputeJob{
		ID:        shared.NewID().String(),
		JobType:   jobType,
		TargetIDs: targetIDs,
		RangeType: rangeType,
		Status:    ComputeJobStatusPending,
		CreatedAt: time.Now().UTC(),
	}
}

func (j *ComputeJob) Normalize() {
	j.ID = strings.TrimSpace(j.ID)
	j.StartTime = strings.TrimSpace(j.StartTime)
	j.EndTime = strings.TrimSpace(j.EndTime)
	j.TriggerReason = strings.TrimSpace(j.TriggerReason)
	j.ErrorMessage = strings.TrimSpace(j.ErrorMessage)
	j.ResultSummary = strings.TrimSpace(j.ResultSummary)
	if j.Priority < 0 {
		j.Priority = 0
	}
	if j.RangeType == "" {
		j.RangeType = ComputeRangeDateRange
	}
	if j.Status == "" {
		j.Status = ComputeJobStatusPending
	}
	normalized := make([]string, 0, len(j.TargetIDs))
	for _, item := range j.TargetIDs {
		item = strings.TrimSpace(item)
		if item != "" {
			normalized = append(normalized, item)
		}
	}
	j.TargetIDs = normalized
}

func (j *ComputeJob) Validate() error {
	if j == nil {
		return shared.NewDomainError(shared.ErrCodeValidation, "compute_job is required", nil)
	}
	j.Normalize()
	if j.ID == "" {
		return shared.NewDomainError(shared.ErrCodeValidation, "job_id is required", nil)
	}
	if len(j.TargetIDs) == 0 {
		return shared.NewDomainError(shared.ErrCodeValidation, "target_ids is required", nil)
	}
	switch j.JobType {
	case ComputeJobTypeFactorRecalculate, ComputeJobTypeSignalRecalculate, ComputeJobTypeUniverseMaterial:
	default:
		return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("unsupported job_type: %s", j.JobType), nil)
	}
	switch j.RangeType {
	case ComputeRangeIncremental, ComputeRangeDateRange, ComputeRangeFullRebuild:
	default:
		return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("unsupported range_type: %s", j.RangeType), nil)
	}
	switch j.Status {
	case ComputeJobStatusPending, ComputeJobStatusQueued, ComputeJobStatusRunning, ComputeJobStatusSucceeded, ComputeJobStatusFailed:
	default:
		return shared.NewDomainError(shared.ErrCodeValidation, fmt.Sprintf("unsupported job status: %s", j.Status), nil)
	}
	return nil
}

func (j *ComputeJob) MarkQueued() {
	j.Status = ComputeJobStatusQueued
}

func (j *ComputeJob) MarkRunning() {
	now := time.Now().UTC()
	j.Status = ComputeJobStatusRunning
	j.StartedAt = &now
}

func (j *ComputeJob) MarkSucceeded(summary string) {
	now := time.Now().UTC()
	j.Status = ComputeJobStatusSucceeded
	j.ResultSummary = strings.TrimSpace(summary)
	j.ErrorMessage = ""
	j.FinishedAt = &now
}

func (j *ComputeJob) MarkFailed(err error) {
	now := time.Now().UTC()
	j.Status = ComputeJobStatusFailed
	if err != nil {
		j.ErrorMessage = err.Error()
	}
	j.FinishedAt = &now
}
