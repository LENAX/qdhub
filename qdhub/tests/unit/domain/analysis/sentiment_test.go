package analysis_test

import (
	"math"
	"testing"

	domain "qdhub/internal/domain/analysis"
)

// ── ValidPopularityRankSrc ────────────────────────────────────────────────────

func TestValidPopularityRankSrc(t *testing.T) {
	valid := []string{"ths", "eastmoney", "kpl"}
	for _, s := range valid {
		if !domain.ValidPopularityRankSrc(s) {
			t.Errorf("expected %q to be valid", s)
		}
	}
	invalid := []string{"", "unknown", "THS", "EASTMONEY"}
	for _, s := range invalid {
		if domain.ValidPopularityRankSrc(s) {
			t.Errorf("expected %q to be invalid", s)
		}
	}
}

// ── resolveWeights ────────────────────────────────────────────────────────────

func TestResolveWeights_Sum(t *testing.T) {
	styles := []domain.SentimentStyle{
		domain.SentimentStyleRelay,
		domain.SentimentStyleBalanced,
		domain.SentimentStyleTrend,
		"",
	}
	for _, style := range styles {
		w := domain.ResolveWeightsForTest(style)
		sum := w.Relay + w.Trend + w.Matrix
		if math.Abs(sum-1.0) > 1e-9 {
			t.Errorf("style=%q weights sum=%.4f, want 1.0", style, sum)
		}
	}
}

// ── computeRelayScore ─────────────────────────────────────────────────────────

func TestComputeRelayScore_ZeroData(t *testing.T) {
	raw := &domain.RelayRawData{}
	score := domain.ComputeRelayScoreForTest(raw)
	if score < 0 || score > 1 {
		t.Errorf("score out of [0,1]: %.4f", score)
	}
}

func TestComputeRelayScore_HighActivity(t *testing.T) {
	raw := &domain.RelayRawData{
		LimitUpCount:        80,
		MaxBoard:            12,
		LadderComplete:      0.9,
		BurstRate:           0.05,
		YesterdayPremiumAvg: 5.0,
		LayerPromotions: []domain.LayerPromotion{
			{Rate: 0.8},
			{Rate: 0.7},
		},
	}
	score := domain.ComputeRelayScoreForTest(raw)
	if score < 0.5 {
		t.Errorf("high activity should yield score >= 0.5, got %.4f", score)
	}
}

func TestComputeRelayScore_LowActivity(t *testing.T) {
	raw := &domain.RelayRawData{
		LimitUpCount:        5,
		MaxBoard:            1,
		LadderComplete:      0.1,
		BurstRate:           0.8,
		YesterdayPremiumAvg: -4.0,
		LayerPromotions: []domain.LayerPromotion{
			{Rate: 0.1},
		},
	}
	score := domain.ComputeRelayScoreForTest(raw)
	if score > 0.4 {
		t.Errorf("low activity should yield score <= 0.4, got %.4f", score)
	}
}

// ── computeTrendScore ─────────────────────────────────────────────────────────

func TestComputeTrendScore_Bullish(t *testing.T) {
	raw := &domain.TrendRawData{
		UpCount: 3800, DownCount: 200, TotalCount: 4000,
		BullMACount: 2000, BullMATotal: 4000,
		NewHighCount: 500, NewHighTotal: 4000,
		TodayHotCodes:     []string{"a", "b", "c"},
		YesterdayHotCodes: []string{"a", "b"},
	}
	score := domain.ComputeTrendScoreForTest(raw)
	if score < 0.5 {
		t.Errorf("bullish market should yield score >= 0.5, got %.4f", score)
	}
}

func TestComputeTrendScore_Bearish(t *testing.T) {
	raw := &domain.TrendRawData{
		UpCount: 300, DownCount: 3700, TotalCount: 4000,
		BullMACount: 200, BullMATotal: 4000,
		NewHighCount: 10, NewHighTotal: 4000,
		TodayHotCodes:     []string{"x", "y"},
		YesterdayHotCodes: []string{"a", "b", "c", "d"},
	}
	score := domain.ComputeTrendScoreForTest(raw)
	if score > 0.4 {
		t.Errorf("bearish market should yield score <= 0.4, got %.4f", score)
	}
}

// ── computeMatrixScore ────────────────────────────────────────────────────────

func TestComputeMatrixScore_BullMarket(t *testing.T) {
	pts := make([]domain.MatrixPoint, 0, 100)
	for i := 0; i < 80; i++ {
		pts = append(pts, domain.MatrixPoint{OpenGap: 0.5, DayReturn: 1.5})
	}
	for i := 0; i < 20; i++ {
		pts = append(pts, domain.MatrixPoint{OpenGap: -0.3, DayReturn: -1.0})
	}
	raw := &domain.MatrixRawData{FullMarket: pts}
	score := domain.ComputeMatrixScoreForTest(raw)
	if score < 0.5 {
		t.Errorf("bull matrix should yield >= 0.5, got %.4f", score)
	}
}

func TestComputeMatrixScore_Empty(t *testing.T) {
	raw := &domain.MatrixRawData{}
	score := domain.ComputeMatrixScoreForTest(raw)
	if score != 0 {
		t.Errorf("empty data should yield 0, got %.4f", score)
	}
}

// ── SentimentLevel ───────────────────────────────────────────────────────────

func TestSentimentLevel(t *testing.T) {
	cases := []struct {
		pct  float64
		want domain.SentimentLevel
	}{
		{0.90, domain.SentimentLevelStrong},
		{0.70, domain.SentimentLevelStrong},
		{0.69, domain.SentimentLevelNeutral},
		{0.50, domain.SentimentLevelNeutral},
		{0.31, domain.SentimentLevelNeutral},
		{0.30, domain.SentimentLevelWeak},
		{0.05, domain.SentimentLevelWeak},
	}
	for _, c := range cases {
		got := domain.SentimentLevelFromPctForTest(c.pct)
		if got != c.want {
			t.Errorf("pct=%.2f: want %q, got %q", c.pct, c.want, got)
		}
	}
}

// ── TemperatureLevel ─────────────────────────────────────────────────────────

func TestTemperatureLevel(t *testing.T) {
	cases := []struct {
		pct  float64
		want domain.TemperatureLevel
	}{
		{0.95, domain.TemperatureLevelBoiling},
		{0.85, domain.TemperatureLevelBoiling},
		{0.84, domain.TemperatureLevelNormal},
		{0.50, domain.TemperatureLevelNormal},
		{0.16, domain.TemperatureLevelNormal},
		{0.15, domain.TemperatureLevelFreezing},
		{0.05, domain.TemperatureLevelFreezing},
	}
	for _, c := range cases {
		got := domain.TemperatureLevelFromPctForTest(c.pct)
		if got != c.want {
			t.Errorf("pct=%.2f: want %q, got %q", c.pct, c.want, got)
		}
	}
}

// ── percentileRankInFloats ───────────────────────────────────────────────────

func TestPercentileRankInFloats(t *testing.T) {
	vals := []float64{1, 2, 3, 4, 5}
	cases := []struct {
		v    float64
		want float64 // [0,1]
	}{
		{1, 0.0},
		{5, 1.0},
		{3, 0.5},
	}
	for _, c := range cases {
		got := domain.PercentileRankInFloatsForTest(vals, c.v)
		if math.Abs(got-c.want) > 0.01 {
			t.Errorf("v=%.1f: want %.2f, got %.2f", c.v, c.want, got)
		}
	}
}

// ── buildMatrixStats ──────────────────────────────────────────────────────────

func TestBuildMatrixStats_QuadrantLabels(t *testing.T) {
	pts := []domain.MatrixPoint{
		{OpenGap: 1.0, DayReturn: 0.5},  // 高开高走
		{OpenGap: -1.0, DayReturn: 0.5}, // 低开高走
		{OpenGap: 1.0, DayReturn: -0.5}, // 高开低走
		{OpenGap: -1.0, DayReturn: -0.5}, // 低开低走
	}
	stats := domain.BuildMatrixStatsForTest(pts)
	if stats.TotalCount != 4 {
		t.Errorf("TotalCount want 4, got %d", stats.TotalCount)
	}
	if len(stats.Quadrants) != 4 {
		t.Errorf("want 4 quadrants, got %d", len(stats.Quadrants))
	}
	for _, q := range stats.Quadrants {
		if q.Count != 1 {
			t.Errorf("quadrant %q: want count=1, got %d", q.Label, q.Count)
		}
		if math.Abs(q.Ratio-0.25) > 0.01 {
			t.Errorf("quadrant %q: want ratio=0.25, got %.2f", q.Label, q.Ratio)
		}
	}
}
