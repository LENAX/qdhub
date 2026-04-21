//go:build !production

package analysis

// 以下函数仅在测试时暴露内部逻辑，生产构建中不包含。

// ResolveWeightsForTest 暴露 resolveWeights
func ResolveWeightsForTest(style SentimentStyle) sentimentWeights {
	return resolveWeights(style)
}

// ComputeRelayScoreForTest 暴露 computeRelayScore
func ComputeRelayScoreForTest(raw *RelayRawData) float64 {
	return computeRelayScore(raw)
}

// ComputeTrendScoreForTest 暴露 computeTrendScore
func ComputeTrendScoreForTest(raw *TrendRawData) float64 {
	return computeTrendScore(raw)
}

// ComputeMatrixScoreForTest 暴露 computeMatrixScore
func ComputeMatrixScoreForTest(raw *MatrixRawData) float64 {
	return computeMatrixScore(raw)
}

// SentimentLevelFromPctForTest 暴露 sentimentLevelFromPct
func SentimentLevelFromPctForTest(pct float64) SentimentLevel {
	return sentimentLevelFromPct(pct)
}

// TemperatureLevelFromPctForTest 暴露 temperatureLevelFromPct
func TemperatureLevelFromPctForTest(pct float64) TemperatureLevel {
	return temperatureLevelFromPct(pct)
}

// PercentileRankInFloatsForTest 暴露 percentileRankInFloats
func PercentileRankInFloatsForTest(vals []float64, current float64) float64 {
	return percentileRankInFloats(vals, current)
}

// BuildMatrixStatsForTest 暴露 buildMatrixStats
func BuildMatrixStatsForTest(pts []MatrixPoint) MatrixStats {
	return buildMatrixStats(pts)
}
