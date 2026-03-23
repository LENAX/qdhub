package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"qdhub/internal/cli/syncstkmin"
)

var (
	syncStkMinsDuckPath   string
	syncStkMinsTable      string
	syncStkMinsStart      string
	syncStkMinsEnd        string
	syncStkMinsFreq       string
	syncStkMinsWinFreq    string
	syncStkMinsListStatus string
	syncStkMinsConc       int
	syncStkMinsInitTable  bool
	syncStkMinsBatchID    string
	syncStkMinsToken      string
	syncStkMinsBaseURL    string
	syncStkMinsRPM        int
)

var syncStkMinsCmd = &cobra.Command{
	Use:   "sync-stk-mins",
	Short: "从 Tushare 同步 stk_mins 到 DuckDB（先拉 stock_basic）",
	Long: `按与任务引擎一致的规则将 [start-date,end-date] 切成 30D 半开时间窗，
映射为每个窗内 API 所需的 yyyy-mm-dd 09:30:00 ~ 15:00:00，并对全市场 ts_code 并发请求 stk_mins。

Tushare 客户端默认全局限流 450 次/分钟；可用 --rate-per-minute 调整。

环境变量 TUSHARE_TOKEN（或 QDHUB_ 前缀由 viper 读取时需注意）可通过 --token 覆盖。`,
	RunE: runSyncStkMins,
}

func init() {
	rootCmd.AddCommand(syncStkMinsCmd)

	syncStkMinsCmd.Flags().StringVar(&syncStkMinsDuckPath, "duckdb-path", "", "DuckDB 文件路径（必填）")
	syncStkMinsCmd.Flags().StringVar(&syncStkMinsTable, "table", "stk_mins", "目标表名")
	syncStkMinsCmd.Flags().StringVar(&syncStkMinsStart, "start-date", "", "起始日期（支持 yyyymmdd 等，与引擎解析一致）")
	syncStkMinsCmd.Flags().StringVar(&syncStkMinsEnd, "end-date", "", "结束日期")
	syncStkMinsCmd.Flags().StringVar(&syncStkMinsFreq, "freq", "1min", "stk_mins 周期参数")
	syncStkMinsCmd.Flags().StringVar(&syncStkMinsWinFreq, "window-freq", "30D", "时间窗步长（与 GenerateDatetimeRange 一致）")
	syncStkMinsCmd.Flags().StringVar(&syncStkMinsListStatus, "list-status", "L", "stock_basic 的 list_status")
	syncStkMinsCmd.Flags().IntVar(&syncStkMinsConc, "concurrency", 16, "并发请求数（与 semaphore 槽位一致）")
	syncStkMinsCmd.Flags().BoolVar(&syncStkMinsInitTable, "init-table", false, "若表不存在则按 8 列官方字段建表")
	syncStkMinsCmd.Flags().StringVar(&syncStkMinsBatchID, "sync-batch-id", "", "非空则写入 sync_batch_id 列（表需含该列）")
	syncStkMinsCmd.Flags().StringVar(&syncStkMinsToken, "token", "", "Tushare token（默认读环境变量 TUSHARE_TOKEN）")
	syncStkMinsCmd.Flags().StringVar(&syncStkMinsBaseURL, "base-url", "", "Tushare API 地址（默认官方）")
	syncStkMinsCmd.Flags().IntVar(&syncStkMinsRPM, "rate-per-minute", 0, "全局限流，0 表示使用客户端默认 450")

	_ = syncStkMinsCmd.MarkFlagRequired("duckdb-path")
	_ = syncStkMinsCmd.MarkFlagRequired("start-date")
	_ = syncStkMinsCmd.MarkFlagRequired("end-date")
}

func runSyncStkMins(_ *cobra.Command, _ []string) error {
	token := strings.TrimSpace(syncStkMinsToken)
	if token == "" {
		token = strings.TrimSpace(os.Getenv("TUSHARE_TOKEN"))
	}
	if token == "" {
		return fmt.Errorf("请设置 --token 或环境变量 TUSHARE_TOKEN")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	path := expandHomePath(syncStkMinsDuckPath)
	if err := syncstkmin.Run(ctx, syncstkmin.Options{
		Token:         token,
		BaseURL:       strings.TrimSpace(syncStkMinsBaseURL),
		DuckDBPath:    path,
		Table:         syncStkMinsTable,
		StartDate:     syncStkMinsStart,
		EndDate:       syncStkMinsEnd,
		Freq:          syncStkMinsFreq,
		WindowFreq:    syncStkMinsWinFreq,
		ListStatus:    syncStkMinsListStatus,
		Concurrency:   syncStkMinsConc,
		InitTable:     syncStkMinsInitTable,
		SyncBatchID:   syncStkMinsBatchID,
		RatePerMinute: syncStkMinsRPM,
	}); err != nil {
		return err
	}
	logrus.Info("sync-stk-mins 成功结束")
	return nil
}
