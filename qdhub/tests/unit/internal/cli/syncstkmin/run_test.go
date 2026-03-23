package syncstkmin_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"qdhub/internal/cli/syncstkmin"
)

func TestRowToStkMins8(t *testing.T) {
	row := map[string]interface{}{
		"ts_code": "000001.SZ", "trade_time": "2020-01-02 09:31:00",
		"open": 10.0, "close": 10.1, "high": 10.2, "low": 9.9,
		"vol": float64(12345), "amount": 1.23e6,
	}
	m, err := syncstkmin.RowToStkMins8(row)
	require.NoError(t, err)
	require.Equal(t, "000001.SZ", m["ts_code"])
	require.Equal(t, int64(12345), m["vol"])
	require.InDelta(t, 1.23e6, m["amount"].(float64), 1e-6)
}

func TestRowToStkMins8VolString(t *testing.T) {
	row := map[string]interface{}{
		"ts_code": "1", "trade_time": "t", "open": 1.0, "close": 1.0, "high": 1.0, "low": 1.0,
		"vol": "999", "amount": 1.0,
	}
	m, err := syncstkmin.RowToStkMins8(row)
	require.NoError(t, err)
	require.Equal(t, int64(999), m["vol"])
}
