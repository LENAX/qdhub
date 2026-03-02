// Package sync 提供同步计划领域逻辑。
// date_required.go：根据计划内 API 的参数名判断是否必须配置日期范围（不依赖基础设施）。

package sync

import "strings"

// dateParamNamePatterns 匹配“与日期/时间范围”相关的参数名（小写后），用于判断是否必须配置日期。
// 仅匹配明确用于范围查询的参数，避免将 list_date 等非范围字段算入。
var dateParamNamePatterns = []string{
	"trade_date", "start_date", "end_date", "start_time", "end_time", "_dt",
}

// PlanRequiresDateRange 根据计划涉及的 API 及其请求参数名，判断是否必须配置日期范围。
// 若任一 API 的任一参数名包含 date/time/dt 等模式，则返回 true。
//
// 入参：
//   - apiNames: 计划要同步的 API 名称列表（一般为 ResolvedAPIs 或 SelectedAPIs）
//   - paramNamesByAPI: key 为 API 名称，value 为该 API 的请求参数名列表（来自 APIMetadata.RequestParams[].Name）
//
// 这样 domain 不依赖 metadata 或 repository，由 application 层注入 paramNamesByAPI。
func PlanRequiresDateRange(apiNames []string, paramNamesByAPI map[string][]string) bool {
	for _, apiName := range apiNames {
		params := paramNamesByAPI[apiName]
		for _, p := range params {
			if isDateLikeParamName(p) {
				return true
			}
		}
	}
	return false
}

// isDateLikeParamName 判断参数名是否与日期/时间相关（小写匹配）。
func isDateLikeParamName(name string) bool {
	lower := strings.ToLower(name)
	for _, pattern := range dateParamNamePatterns {
		if strings.Contains(lower, pattern) {
			return true
		}
	}
	return false
}
