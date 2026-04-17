package metrics

import (
	"context"
	"fmt"
	"time"

	"qdhub/internal/domain/datastore"
	domain "qdhub/internal/domain/metrics"
	"qdhub/internal/domain/shared"
)

type ComputeJobRepoDuckDB struct {
	db datastore.QuantDB
}

func NewComputeJobRepoDuckDB(db datastore.QuantDB) *ComputeJobRepoDuckDB {
	return &ComputeJobRepoDuckDB{db: db}
}

func (r *ComputeJobRepoDuckDB) Save(ctx context.Context, job *domain.ComputeJob) error {
	if err := job.Validate(); err != nil {
		return err
	}
	sql := `INSERT INTO compute_job (
		job_id, job_type, target_ids_json, range_type, start_time, end_time, trigger_reason, priority,
		status, error_message, result_summary, created_at, started_at, finished_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(job_id) DO UPDATE SET
		job_type = EXCLUDED.job_type,
		target_ids_json = EXCLUDED.target_ids_json,
		range_type = EXCLUDED.range_type,
		start_time = EXCLUDED.start_time,
		end_time = EXCLUDED.end_time,
		trigger_reason = EXCLUDED.trigger_reason,
		priority = EXCLUDED.priority,
		status = EXCLUDED.status,
		error_message = EXCLUDED.error_message,
		result_summary = EXCLUDED.result_summary,
		started_at = EXCLUDED.started_at,
		finished_at = EXCLUDED.finished_at`
	var startedAt any
	var finishedAt any
	if job.StartedAt != nil {
		startedAt = job.StartedAt.Format(time.RFC3339Nano)
	}
	if job.FinishedAt != nil {
		finishedAt = job.FinishedAt.Format(time.RFC3339Nano)
	}
	_, err := r.db.Execute(ctx, sql,
		job.ID,
		string(job.JobType),
		mustJSON(job.TargetIDs),
		string(job.RangeType),
		job.StartTime,
		job.EndTime,
		job.TriggerReason,
		job.Priority,
		string(job.Status),
		job.ErrorMessage,
		job.ResultSummary,
		job.CreatedAt.Format(time.RFC3339Nano),
		startedAt,
		finishedAt,
	)
	if err != nil {
		return fmt.Errorf("save compute_job: %w", err)
	}
	return nil
}

func (r *ComputeJobRepoDuckDB) Get(ctx context.Context, jobID string) (*domain.ComputeJob, error) {
	rows, err := r.db.Query(ctx, `SELECT * FROM compute_job WHERE job_id = ?`, jobID)
	if err != nil {
		return nil, fmt.Errorf("get compute_job: %w", err)
	}
	if len(rows) == 0 {
		return nil, shared.NewDomainError(shared.ErrCodeNotFound, "job not found", nil)
	}
	row := rows[0]
	job := &domain.ComputeJob{
		ID:            toString(row["job_id"]),
		JobType:       domain.ComputeJobType(toString(row["job_type"])),
		RangeType:     domain.ComputeRangeType(toString(row["range_type"])),
		StartTime:     toString(row["start_time"]),
		EndTime:       toString(row["end_time"]),
		TriggerReason: toString(row["trigger_reason"]),
		Priority:      toInt(row["priority"]),
		Status:        domain.ComputeJobStatus(toString(row["status"])),
		ErrorMessage:  toString(row["error_message"]),
		ResultSummary: toString(row["result_summary"]),
	}
	if err := decodeJSON(row["target_ids_json"], &job.TargetIDs); err != nil {
		return nil, fmt.Errorf("decode target_ids_json: %w", err)
	}
	if created := toTimePtr(row["created_at"]); created != nil {
		job.CreatedAt = created.UTC()
	}
	job.StartedAt = toTimePtr(row["started_at"])
	job.FinishedAt = toTimePtr(row["finished_at"])
	return job, nil
}
