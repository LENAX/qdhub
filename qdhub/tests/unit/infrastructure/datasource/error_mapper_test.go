package datasource_test

import (
	"testing"

	"qdhub/internal/infrastructure/datasource"
)

func TestErrorMapper_MapError_BySourceCode(t *testing.T) {
	rules := []datasource.ErrorMappingRule{
		{
			SourceCodes: []int{-1001, -1002},
			TargetCode:  datasource.ErrCodeTokenInvalid,
		},
		{
			SourceCodes: []int{-1003},
			TargetCode:  datasource.ErrCodeRateLimited,
		},
	}

	mapper := datasource.NewErrorMapper(rules, datasource.ErrCodeUnknown)

	tests := []struct {
		name       string
		sourceCode int
		msg        string
		wantCode   string
	}{
		{"token invalid -1001", -1001, "token无效", datasource.ErrCodeTokenInvalid},
		{"token invalid -1002", -1002, "token过期", datasource.ErrCodeTokenInvalid},
		{"rate limited", -1003, "请求过于频繁", datasource.ErrCodeRateLimited},
		{"unknown code", -9999, "未知错误", datasource.ErrCodeUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mapper.MapError(tt.sourceCode, tt.msg)
			if err.Code != tt.wantCode {
				t.Errorf("MapError() code = %s, want %s", err.Code, tt.wantCode)
			}
			if err.Message != tt.msg {
				t.Errorf("MapError() message = %s, want %s", err.Message, tt.msg)
			}
		})
	}
}

func TestErrorMapper_MapError_ByKeywords(t *testing.T) {
	rules := []datasource.ErrorMappingRule{
		// 每日限制优先（带关键词）
		{
			SourceCodes: []int{-2003},
			Keywords:    []string{"每日", "daily"},
			TargetCode:  datasource.ErrCodeDailyLimitExceeded,
		},
		// 每分钟限制（带关键词）
		{
			SourceCodes: []int{-2003},
			Keywords:    []string{"每分钟", "minute"},
			TargetCode:  datasource.ErrCodeRateLimited,
		},
		// -2003 默认
		{
			SourceCodes: []int{-2003},
			TargetCode:  datasource.ErrCodeRateLimited,
		},
	}

	mapper := datasource.NewErrorMapper(rules, datasource.ErrCodeUnknown)

	tests := []struct {
		name     string
		msg      string
		wantCode string
	}{
		{"daily limit zh", "每日请求已达到上限", datasource.ErrCodeDailyLimitExceeded},
		{"daily limit en", "daily limit exceeded", datasource.ErrCodeDailyLimitExceeded},
		{"minute limit zh", "每分钟请求已达到上限", datasource.ErrCodeRateLimited},
		{"minute limit en", "minute rate limit", datasource.ErrCodeRateLimited},
		{"default rate limited", "请求超限", datasource.ErrCodeRateLimited},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mapper.MapError(-2003, tt.msg)
			if err.Code != tt.wantCode {
				t.Errorf("MapError() code = %s, want %s", err.Code, tt.wantCode)
			}
		})
	}
}

func TestErrorMapper_MapError_PriorityOrder(t *testing.T) {
	// 测试规则优先级：先匹配的优先
	rules := []datasource.ErrorMappingRule{
		// 特定消息优先匹配
		{
			SourceCodes: []int{-1000},
			Keywords:    []string{"特殊"},
			TargetCode:  datasource.ErrCodePermissionDeny,
		},
		// 通用匹配
		{
			SourceCodes: []int{-1000},
			TargetCode:  datasource.ErrCodeUnknown,
		},
	}

	mapper := datasource.NewErrorMapper(rules, datasource.ErrCodeUnknown)

	// 带特殊关键词应该匹配第一条规则
	err1 := mapper.MapError(-1000, "这是特殊错误")
	if err1.Code != datasource.ErrCodePermissionDeny {
		t.Errorf("expected %s, got %s", datasource.ErrCodePermissionDeny, err1.Code)
	}

	// 不带关键词应该匹配第二条规则
	err2 := mapper.MapError(-1000, "普通错误")
	if err2.Code != datasource.ErrCodeUnknown {
		t.Errorf("expected %s, got %s", datasource.ErrCodeUnknown, err2.Code)
	}
}

func TestErrorMapper_MapError_EmptyRules(t *testing.T) {
	mapper := datasource.NewErrorMapper(nil, datasource.ErrCodeUnknown)

	err := mapper.MapError(-1000, "任意错误")
	if err.Code != datasource.ErrCodeUnknown {
		t.Errorf("expected %s, got %s", datasource.ErrCodeUnknown, err.Code)
	}
}

func TestErrorMapper_MapError_DefaultCode(t *testing.T) {
	// 测试自定义默认错误码
	mapper := datasource.NewErrorMapper(nil, datasource.ErrCodeAPINotFound)

	err := mapper.MapError(-1000, "未知错误")
	if err.Code != datasource.ErrCodeAPINotFound {
		t.Errorf("expected %s, got %s", datasource.ErrCodeAPINotFound, err.Code)
	}
}

func TestErrorMapper_MapError_NoSourceCodeFilter(t *testing.T) {
	// 测试不限定错误码，仅按关键词匹配
	rules := []datasource.ErrorMappingRule{
		{
			Keywords:   []string{"network", "网络"},
			TargetCode: datasource.ErrCodeNetworkError,
		},
	}

	mapper := datasource.NewErrorMapper(rules, datasource.ErrCodeUnknown)

	// 任意错误码 + 网络关键词应该匹配
	err1 := mapper.MapError(-999, "网络连接失败")
	if err1.Code != datasource.ErrCodeNetworkError {
		t.Errorf("expected %s, got %s", datasource.ErrCodeNetworkError, err1.Code)
	}

	err2 := mapper.MapError(-888, "network timeout")
	if err2.Code != datasource.ErrCodeNetworkError {
		t.Errorf("expected %s, got %s", datasource.ErrCodeNetworkError, err2.Code)
	}

	// 无关键词应该返回默认
	err3 := mapper.MapError(-777, "其他错误")
	if err3.Code != datasource.ErrCodeUnknown {
		t.Errorf("expected %s, got %s", datasource.ErrCodeUnknown, err3.Code)
	}
}
