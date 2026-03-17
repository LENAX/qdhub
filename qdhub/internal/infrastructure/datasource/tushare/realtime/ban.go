package realtime

import (
	"strings"
)

// IsRealtimeBanError 判断是否为实时行情接口的「被 ban/多 IP 限制」类错误。
// 此类错误应导致工作流立即取消并返回明确错误信息，而非重试。
//
// 说明：Tushare 实时接口使用 WebSocket，无 HTTP 状态码，仅能通过上游返回的文案判断
//（如「在线Ip数量超限, 请联系管理员获取权限」）。HTTP 类源（新浪/东财）则可能返回 403，
// 两者均通过下方关键词或 403 字符串匹配。
func IsRealtimeBanError(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	// HTTP 类源（sina/eastmoney）：状态 403 表示被拒绝
	if strings.Contains(s, "403") {
		return true
	}
	// 常见 ban/限流文案（含 Tushare WS「在线Ip数量超限」等，WebSocket 无状态码，靠文案识别）
	keywords := []string{"ban", "禁止", "多ip", "多 ip", "限流", "forbidden", "超限", "权限", "没有订阅数据", "在线ip", "ip数量"}
	for _, k := range keywords {
		if strings.Contains(s, k) {
			return true
		}
	}
	return false
}
