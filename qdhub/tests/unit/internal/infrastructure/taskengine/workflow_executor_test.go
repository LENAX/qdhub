package taskengine_test

import (
	"testing"

	"qdhub/internal/infrastructure/taskengine"
)

func TestMergeTushareProxyConfig(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name            string
		dbWS, dbRSA     string
		envWS, envRSA   string
		wantWS, wantRSA string
	}{
		{"both_env_win", "", "", "ws://a/r", "/k/pub.pem", "ws://a/r", "/k/pub.pem"},
		{"db_only", "ws://b/r", "/d/p.pem", "", "", "ws://b/r", "/d/p.pem"},
		{"env_fills_db", "ws://b/r", "", "", "/e/p.pem", "ws://b/r", "/e/p.pem"},
		{"env_fills_ws", "", "/d/p.pem", "ws://c/r", "", "ws://c/r", "/d/p.pem"},
		{"empty", "", "", "", "", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotWS, gotRSA := taskengine.MergeTushareProxyConfig(tc.dbWS, tc.dbRSA, tc.envWS, tc.envRSA)
			if gotWS != tc.wantWS || gotRSA != tc.wantRSA {
				t.Fatalf("merge(%q,%q,%q,%q) = (%q,%q), want (%q,%q)",
					tc.dbWS, tc.dbRSA, tc.envWS, tc.envRSA, gotWS, gotRSA, tc.wantWS, tc.wantRSA)
			}
		})
	}
}
