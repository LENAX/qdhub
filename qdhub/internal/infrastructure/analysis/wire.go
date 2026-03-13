package analysis

import (
	"qdhub/internal/domain/analysis"
)

// NewAnalysisServiceFromReaders 从 *Readers 组装领域 AnalysisService，供 container 注入
func NewAnalysisServiceFromReaders(r *Readers) analysis.AnalysisService {
	return analysis.NewAnalysisService(
		r,
		r,
		r,
		&limitLadderReaderImpl{r},
		&limitComparisonReaderImpl{r},
		&sectorLimitStatsReaderImpl{r},
		&sectorLimitStocksReaderImpl{r},
		r,
		r,
		r,
		&stockListReaderImpl{r},
		r,
		&indexListReaderImpl{r},
		&conceptListReaderImpl{r},
		&dragonTigerReaderImpl{r},
		r,
		r, // MoneyFlowConceptReader
		r,
		&newsReaderImpl{r},
		&limitUpListReaderImpl{r},
		&limitUpLadderReaderImpl{r},
		&limitUpComparisonReaderImpl{r},
		&limitUpBySectorReaderImpl{r},
		&FirstLimitUpReaderImpl{r},
		r,
		r,
		r,
		r,
		r,
		r, // TradeCalendarReader
		r, // RealtimeTickReader
		r, // IntradayTickReader
		r, // IntradayKlineReader
	)
}
