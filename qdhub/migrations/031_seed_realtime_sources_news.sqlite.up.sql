-- Seed realtime_sources: news (新闻)，pull-based，每 5 分钟由 SyncPlan 调度更新，写入独立实时库以减轻主库压力。
-- Version: 031
-- Description: 新增 type=news 的实时数据源记录；与 ts_proxy/sina/tushare_ws 并列，使用独立实时库 realtime_duckdb。

INSERT OR IGNORE INTO realtime_sources (
    id, name, type, config, priority, is_primary, health_check_on_startup, enabled,
    last_health_status, last_health_at, last_health_error, created_at, updated_at
)
VALUES
    (
        'aaaaaaaa-0005-4000-8000-000000000005',
        'news',
        'news',
        '{"freq":"5MIN"}',
        5,
        0,
        0,
        1,
        NULL,
        NULL,
        '',
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP
    );
