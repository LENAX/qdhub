package sync_test

import (
	"testing"

	"qdhub/internal/domain/shared"
	"qdhub/internal/domain/sync"
)

func TestSyncJobValidator_ValidateSyncJob(t *testing.T) {
	validator := sync.NewSyncJobValidator()

	validJob := func() *sync.SyncJob {
		cronExpr := "0 0 9 * * *"
		return &sync.SyncJob{
			ID:             shared.NewID(),
			Name:           "Daily Stock Sync",
			Description:    "每日同步股票数据",
			APIMetadataID:  shared.NewID(),
			DataStoreID:    shared.NewID(),
			WorkflowDefID:  shared.NewID(),
			Mode:           sync.SyncModeBatch,
			CronExpression: &cronExpr,
			Params: map[string]interface{}{
				"start_date": "20240101",
			},
			ParamRules: []sync.ParamRule{
				{ParamName: "ts_code", RuleType: "required", RuleConfig: nil},
			},
			Status: sync.JobStatusEnabled,
		}
	}

	tests := []struct {
		name    string
		modify  func(*sync.SyncJob)
		wantErr bool
	}{
		{
			name:    "valid sync job",
			modify:  func(j *sync.SyncJob) {},
			wantErr: false,
		},
		{
			name:    "nil sync job",
			modify:  func(j *sync.SyncJob) {},
			wantErr: true,
		},
		{
			name:    "empty ID",
			modify:  func(j *sync.SyncJob) { j.ID = "" },
			wantErr: true,
		},
		{
			name:    "empty name",
			modify:  func(j *sync.SyncJob) { j.Name = "" },
			wantErr: true,
		},
		{
			name:    "empty APIMetadataID",
			modify:  func(j *sync.SyncJob) { j.APIMetadataID = "" },
			wantErr: true,
		},
		{
			name:    "empty DataStoreID",
			modify:  func(j *sync.SyncJob) { j.DataStoreID = "" },
			wantErr: true,
		},
		{
			name:    "empty WorkflowDefID",
			modify:  func(j *sync.SyncJob) { j.WorkflowDefID = "" },
			wantErr: true,
		},
		{
			name:    "invalid mode",
			modify:  func(j *sync.SyncJob) { j.Mode = "invalid" },
			wantErr: true,
		},
		{
			name:    "valid realtime mode",
			modify:  func(j *sync.SyncJob) { j.Mode = sync.SyncModeRealtime },
			wantErr: false,
		},
		{
			name: "invalid cron expression",
			modify: func(j *sync.SyncJob) {
				invalid := "invalid cron"
				j.CronExpression = &invalid
			},
			wantErr: true,
		},
		{
			name:    "nil cron expression is valid",
			modify:  func(j *sync.SyncJob) { j.CronExpression = nil },
			wantErr: false,
		},
		{
			name:    "invalid status",
			modify:  func(j *sync.SyncJob) { j.Status = "invalid" },
			wantErr: true,
		},
		{
			name:    "running status is valid",
			modify:  func(j *sync.SyncJob) { j.Status = sync.JobStatusRunning },
			wantErr: false,
		},
		{
			name:    "disabled status is valid",
			modify:  func(j *sync.SyncJob) { j.Status = sync.JobStatusDisabled },
			wantErr: false,
		},
		{
			name: "empty param rule name",
			modify: func(j *sync.SyncJob) {
				j.ParamRules = []sync.ParamRule{{ParamName: "", RuleType: "required"}}
			},
			wantErr: true,
		},
		{
			name: "empty param rule type",
			modify: func(j *sync.SyncJob) {
				j.ParamRules = []sync.ParamRule{{ParamName: "ts_code", RuleType: ""}}
			},
			wantErr: true,
		},
		{
			name:    "empty param rules is valid",
			modify:  func(j *sync.SyncJob) { j.ParamRules = []sync.ParamRule{} },
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var job *sync.SyncJob
			if tt.name == "nil sync job" {
				job = nil
			} else {
				job = validJob()
				tt.modify(job)
			}

			err := validator.ValidateSyncJob(job)

			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateSyncJob() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSyncJobValidator_ValidateJobParams(t *testing.T) {
	validator := sync.NewSyncJobValidator()

	tests := []struct {
		name       string
		params     map[string]interface{}
		paramRules []sync.ParamRule
		wantErr    bool
	}{
		{
			name:       "valid params with required rule",
			params:     map[string]interface{}{"ts_code": "000001.SZ"},
			paramRules: []sync.ParamRule{{ParamName: "ts_code", RuleType: "required"}},
			wantErr:    false,
		},
		{
			name:       "missing required param",
			params:     map[string]interface{}{},
			paramRules: []sync.ParamRule{{ParamName: "ts_code", RuleType: "required"}},
			wantErr:    true,
		},
		{
			name:       "nil params",
			params:     nil,
			paramRules: []sync.ParamRule{},
			wantErr:    true,
		},
		{
			name:       "empty params with no required rules",
			params:     map[string]interface{}{},
			paramRules: []sync.ParamRule{{ParamName: "ts_code", RuleType: "optional"}},
			wantErr:    false,
		},
		{
			name:       "empty params and empty rules",
			params:     map[string]interface{}{},
			paramRules: []sync.ParamRule{},
			wantErr:    false,
		},
		{
			name:       "extra params not in rules is valid",
			params:     map[string]interface{}{"ts_code": "000001.SZ", "extra": "value"},
			paramRules: []sync.ParamRule{{ParamName: "ts_code", RuleType: "required"}},
			wantErr:    false,
		},
		{
			name:   "multiple required params - all present",
			params: map[string]interface{}{"ts_code": "000001.SZ", "trade_date": "20240101"},
			paramRules: []sync.ParamRule{
				{ParamName: "ts_code", RuleType: "required"},
				{ParamName: "trade_date", RuleType: "required"},
			},
			wantErr: false,
		},
		{
			name:   "multiple required params - one missing",
			params: map[string]interface{}{"ts_code": "000001.SZ"},
			paramRules: []sync.ParamRule{
				{ParamName: "ts_code", RuleType: "required"},
				{ParamName: "trade_date", RuleType: "required"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.ValidateJobParams(tt.params, tt.paramRules)

			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateJobParams() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSyncJobValidator_ValidateCronExpression(t *testing.T) {
	validator := sync.NewSyncJobValidator()

	tests := []struct {
		name     string
		cronExpr string
		wantErr  bool
	}{
		{
			name:     "valid 5-field cron",
			cronExpr: "0 9 * * *",
			wantErr:  false,
		},
		{
			name:     "valid 6-field cron with seconds",
			cronExpr: "0 0 9 * * *",
			wantErr:  false,
		},
		{
			name:     "empty cron expression",
			cronExpr: "",
			wantErr:  true,
		},
		{
			name:     "whitespace only",
			cronExpr: "   ",
			wantErr:  true,
		},
		{
			name:     "too few fields",
			cronExpr: "0 9 * *",
			wantErr:  true,
		},
		{
			name:     "too many fields",
			cronExpr: "0 0 9 * * * *",
			wantErr:  true,
		},
		{
			name:     "valid with wildcards",
			cronExpr: "* * * * *",
			wantErr:  false,
		},
		{
			name:     "valid with ranges",
			cronExpr: "0-30 9-17 * * 1-5",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validator.ValidateCronExpression(tt.cronExpr)

			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateCronExpression() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
