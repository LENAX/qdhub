// Package workflow 内工作流/任务状态判断（大小写不敏感）
package workflow

import "strings"

// isOneOf 判断 s 是否与任一 variant 相等（忽略大小写）
func isOneOf(s string, variants ...string) bool {
	for _, v := range variants {
		if strings.EqualFold(s, v) {
			return true
		}
	}
	return false
}

// IsSuccess 表示成功态：Success, SUCCESS, success, Completed, COMPLETED 等
func IsSuccess(s string) bool {
	return isOneOf(s, "success", "completed")
}

// IsFailed 表示失败/错误态：Failed, FAILED, failed, Fail, FAIL, Error, ERROR 等
func IsFailed(s string) bool {
	return isOneOf(s, "failed", "fail", "error")
}

// IsRunning 表示运行中：Running, RUNNING, running
func IsRunning(s string) bool {
	return isOneOf(s, "running")
}

// IsPending 表示等待/就绪：Pending, PENDING, Ready, READY, pending, ready
func IsPending(s string) bool {
	return isOneOf(s, "pending", "ready")
}

// IsTerminated 表示已终止/取消：Terminated, TERMINATED, Cancelled, CANCELLED, cancelled
func IsTerminated(s string) bool {
	return isOneOf(s, "terminated", "cancelled")
}

// IsPaused 表示已暂停：Paused, PAUSED, paused
func IsPaused(s string) bool {
	return isOneOf(s, "paused")
}

// IsSkipped 表示已跳过：Skipped, SKIPPED, skipped
func IsSkipped(s string) bool {
	return isOneOf(s, "skipped")
}

// IsTerminal 表示工作流实例终态（成功、失败或终止之一）
func IsTerminal(s string) bool {
	return IsSuccess(s) || IsFailed(s) || IsTerminated(s)
}

// IsValidInstanceStatus 表示合法的实例状态（用于校验，大小写不敏感）
func IsValidInstanceStatus(s string) bool {
	return IsPending(s) || IsRunning(s) || IsPaused(s) || IsSuccess(s) || IsFailed(s) || IsTerminated(s)
}
