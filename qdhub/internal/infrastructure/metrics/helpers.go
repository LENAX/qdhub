package metrics

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func mustJSON(value any) string {
	if value == nil {
		return "null"
	}
	data, err := json.Marshal(value)
	if err != nil {
		return "null"
	}
	return string(data)
}

func decodeJSON[T any](raw any, out *T) error {
	if raw == nil {
		return nil
	}
	text := strings.TrimSpace(toString(raw))
	if text == "" || text == "null" {
		return nil
	}
	return json.Unmarshal([]byte(text), out)
}

func toString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	case fmt.Stringer:
		return x.String()
	default:
		return fmt.Sprintf("%v", x)
	}
}

func toInt(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case int32:
		return int(x)
	case int64:
		return int(x)
	case float32:
		return int(x)
	case float64:
		return int(x)
	case string:
		n, _ := strconv.Atoi(strings.TrimSpace(x))
		return n
	default:
		n, _ := strconv.Atoi(toString(v))
		return n
	}
}

func toFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case string:
		n, _ := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return n
	default:
		n, _ := strconv.ParseFloat(toString(v), 64)
		return n
	}
}

func toBoolPtr(v any) *bool {
	if v == nil {
		return nil
	}
	switch x := v.(type) {
	case bool:
		return &x
	case string:
		trimmed := strings.TrimSpace(strings.ToLower(x))
		if trimmed == "" {
			return nil
		}
		b := trimmed == "true" || trimmed == "1"
		return &b
	default:
		text := strings.TrimSpace(strings.ToLower(toString(v)))
		if text == "" {
			return nil
		}
		b := text == "true" || text == "1"
		return &b
	}
}

func toTimePtr(v any) *time.Time {
	text := strings.TrimSpace(toString(v))
	if text == "" || strings.EqualFold(text, "null") {
		return nil
	}
	layouts := []string{time.RFC3339Nano, "2006-01-02 15:04:05", time.RFC3339}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, text); err == nil {
			return &parsed
		}
	}
	return nil
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	items := make([]string, n)
	for i := range items {
		items[i] = "?"
	}
	return strings.Join(items, ", ")
}
