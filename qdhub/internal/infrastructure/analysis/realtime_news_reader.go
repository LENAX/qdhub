package analysis

import (
	"context"
	"fmt"
	"strings"

	"qdhub/internal/domain/analysis"
	"qdhub/internal/domain/datastore"
)

// RealtimeNewsReader 从 realtime DuckDB 的 news 表读取新闻，供 /analysis/news/stream 使用。
// 表结构见 quantdb/realtime_migration：datetime, content, title, channels。
var _ analysis.NewsReader = (*RealtimeNewsReader)(nil)

// RealtimeNewsReader 实现 NewsReader，仅查 realtime 库的 news 表。
type RealtimeNewsReader struct {
	db datastore.QuantDB
}

// NewRealtimeNewsReader 创建从给定 QuantDB（realtime DuckDB）读 news 的 Reader。
func NewRealtimeNewsReader(db datastore.QuantDB) *RealtimeNewsReader {
	return &RealtimeNewsReader{db: db}
}

// List 实现 NewsReader.List：仅查 news 表，支持 order/limit/offset、start_date/end_date、sources(channels)。
func (r *RealtimeNewsReader) List(ctx context.Context, req analysis.NewsListRequest) ([]analysis.NewsItem, error) {
	ok, _ := r.db.TableExists(ctx, "news")
	if !ok {
		return nil, nil
	}
	order := "DESC"
	if strings.TrimSpace(strings.ToLower(req.Order)) == "time_asc" {
		order = "ASC"
	}
	sql := "SELECT MAX(title) AS title, content, MAX(channels) AS source, datetime AS publish_time FROM news WHERE 1=1"
	args := []any{}
	if req.StartDate != nil && *req.StartDate != "" {
		sql += " AND datetime >= ?"
		args = append(args, *req.StartDate)
	}
	if req.EndDate != nil && *req.EndDate != "" {
		sql += " AND datetime <= ?"
		args = append(args, *req.EndDate)
	}
	if req.Sources != nil && strings.TrimSpace(*req.Sources) != "" {
		parts := strings.Split(*req.Sources, ",")
		for i, p := range parts {
			parts[i] = strings.TrimSpace(p)
		}
		if len(parts) > 0 {
			ph := strings.Repeat("?,", len(parts))
			sql += " AND channels IN (" + ph[:len(ph)-1] + ")"
			for _, p := range parts {
				args = append(args, p)
			}
		}
	}
	sql += " GROUP BY content, datetime ORDER BY datetime " + order + " LIMIT ? OFFSET ?"
	limit, offset := req.Limit, req.Offset
	if limit <= 0 {
		limit = 50
	}
	args = append(args, limit, offset)
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	out := make([]analysis.NewsItem, 0, len(rows))
	for _, m := range rows {
		out = append(out, analysis.NewsItem{
			ID:          "",
			Title:       strFromMap(m, "title"),
			Content:     strFromMap(m, "content"),
			Source:      strFromMap(m, "source"),
			PublishTime: strFromMap(m, "publish_time"),
			Author:      "",
			Category:    "",
		})
	}
	return out, nil
}

func strFromMap(m map[string]any, key string) string {
	if v, ok := m[key]; ok && v != nil {
		return strings.TrimSpace(fmt.Sprint(v))
	}
	return ""
}
