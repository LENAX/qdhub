#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from collections import deque
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import tushare as ts
import typer
from pydantic import BaseModel, ConfigDict, Field, field_validator


DATE_TIME_LAYOUTS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d",
    "%Y%m%d %H%M%S",
    "%Y%m%d",
)

app = typer.Typer(add_completion=False, help="从 Tushare 同步 stk_mins 到 DuckDB（Python 版本）")
logger = logging.getLogger("tushare_sync_stk_mins")


class SyncConfig(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    token: str
    duckdb_path: str
    table: str = "stk_mins"
    start_date: str
    end_date: str
    freq: str = "1min"
    window_freq: str = "30D"
    list_status: str = "L"
    concurrency: int = Field(default=16, ge=1, le=256)
    rate_per_minute: int = Field(default=450, ge=1, le=10000)
    chunk_size: int = Field(default=200, ge=10, le=2000)
    init_table: bool = False
    stock_basic_page_size: int = Field(default=5000, ge=100, le=10000)
    progress_report_seconds: float = Field(default=10.0, ge=1.0, le=300.0)

    @field_validator("table")
    @classmethod
    def validate_table_name(cls, value: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9_]+", value):
            raise ValueError("table 仅允许字母/数字/下划线")
        return value

    @field_validator("window_freq")
    @classmethod
    def validate_window_freq(cls, value: str) -> str:
        if value.upper() != "30D":
            raise ValueError("当前脚本仅支持 window_freq=30D")
        return value


class HalfOpenWindow(BaseModel):
    start: datetime
    end: datetime


class APIRange(BaseModel):
    start_date: str
    end_date: str


class StkMinsRow(BaseModel):
    model_config = ConfigDict(extra="ignore")

    ts_code: str
    trade_time: str
    open: float
    close: float
    high: float
    low: float
    vol: int
    amount: float

    @field_validator("vol", mode="before")
    @classmethod
    def normalize_vol(cls, value: Any) -> int:
        if value is None or value == "":
            return 0
        if isinstance(value, str):
            value = value.strip()
            if value == "":
                return 0
        return int(float(value))


class AsyncRateLimiter:
    def __init__(self, rate_per_minute: int) -> None:
        self.rate_per_minute = rate_per_minute
        self._events: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                while self._events and now - self._events[0] >= 60:
                    self._events.popleft()
                if len(self._events) < self.rate_per_minute:
                    self._events.append(now)
                    return
                wait_seconds = 60 - (now - self._events[0]) + 0.01
            await asyncio.sleep(max(wait_seconds, 0.01))


class ProgressReporter:
    def __init__(self, total: int, report_every_seconds: float = 10.0) -> None:
        self.total = max(total, 1)
        self.report_every_seconds = max(report_every_seconds, 1.0)
        self.done = 0
        self.success = 0
        self.failed = 0
        self.fetched_rows = 0
        self.inserted_rows = 0
        self.start_monotonic = time.monotonic()
        self.last_report_monotonic = self.start_monotonic

    def advance(
        self,
        count: int,
        *,
        success: int = 0,
        failed: int = 0,
        fetched_rows: int = 0,
        inserted_rows: int = 0,
    ) -> None:
        self.done += max(count, 0)
        self.success += max(success, 0)
        self.failed += max(failed, 0)
        self.fetched_rows += max(fetched_rows, 0)
        self.inserted_rows += max(inserted_rows, 0)
        self.maybe_report()

    def maybe_report(self, force: bool = False) -> None:
        now = time.monotonic()
        if not force and (now - self.last_report_monotonic) < self.report_every_seconds:
            return
        elapsed = max(now - self.start_monotonic, 1e-6)
        pct = min(100.0, (self.done / self.total) * 100.0)
        speed = self.done / elapsed
        insert_speed = self.inserted_rows / elapsed
        remaining = max(self.total - self.done, 0)
        eta_seconds = int(remaining / speed) if speed > 0 else -1
        eta_text = f"{eta_seconds}s" if eta_seconds >= 0 else "unknown"
        logger.info(
            "进度: %s/%s (%.2f%%) | 请求成功/失败=%s/%s | fetched/inserted=%s/%s | 任务速率=%.2f task/s | 写入速率=%.2f row/s | ETA=%s",
            self.done,
            self.total,
            pct,
            self.success,
            self.failed,
            self.fetched_rows,
            self.inserted_rows,
            speed,
            insert_speed,
            eta_text,
        )
        self.last_report_monotonic = now


def parse_flexible_datetime(raw: str) -> datetime:
    text = raw.strip()
    for layout in DATE_TIME_LAYOUTS:
        try:
            return datetime.strptime(text, layout)
        except ValueError:
            continue
    raise ValueError(f"不支持的日期格式: {raw}")


def stk_mins_step_span(start_raw: str, end_raw: str) -> tuple[datetime, datetime]:
    start_dt = parse_flexible_datetime(start_raw)
    end_dt = parse_flexible_datetime(end_raw)
    if end_dt < start_dt:
        raise ValueError("end_date 必须大于等于 start_date")
    start_day = datetime(start_dt.year, start_dt.month, start_dt.day, 0, 0, 0)
    end_day_exclusive = datetime(end_dt.year, end_dt.month, end_dt.day, 0, 0, 0) + timedelta(days=1)
    return start_day, end_day_exclusive


def generate_30d_half_open_windows(start: datetime, end_exclusive: datetime) -> list[HalfOpenWindow]:
    windows: list[HalfOpenWindow] = []
    current = start
    step = timedelta(days=30)
    while current < end_exclusive:
        next_dt = min(current + step, end_exclusive)
        windows.append(HalfOpenWindow(start=current, end=next_dt))
        current = next_dt
    return windows


def api_range_from_half_open_window(window: HalfOpenWindow) -> APIRange:
    start_out = datetime(window.start.year, window.start.month, window.start.day, 9, 30, 0)
    if window.end <= window.start:
        end_out = start_out
    else:
        last_inclusive = window.end - timedelta(seconds=1)
        end_out = datetime(last_inclusive.year, last_inclusive.month, last_inclusive.day, 15, 0, 0)
        if end_out < start_out:
            end_out = start_out
    return APIRange(
        start_date=start_out.strftime("%Y-%m-%d %H:%M:%S"),
        end_date=end_out.strftime("%Y-%m-%d %H:%M:%S"),
    )


def chunked(items: list[str], chunk_size: int) -> list[list[str]]:
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def fetch_stock_basic_codes(pro: Any, list_status: str, page_size: int) -> list[str]:
    all_codes: list[str] = []
    offset = 0
    while True:
        df = pro.stock_basic(
            exchange="",
            list_status=list_status,
            fields="ts_code",
            offset=offset,
            limit=page_size,
        )
        if df is None or df.empty:
            break
        page_codes = [str(v).strip() for v in df["ts_code"].tolist() if str(v).strip()]
        all_codes.extend(page_codes)
        if len(df) < page_size:
            break
        offset += page_size
    return sorted(set(all_codes))


def to_rows_dataframe(records: list[dict[str, Any]]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=["ts_code", "trade_time", "open", "close", "high", "low", "vol", "amount"])
    rows = [StkMinsRow.model_validate(rec).model_dump() for rec in records]
    return pd.DataFrame(rows, columns=["ts_code", "trade_time", "open", "close", "high", "low", "vol", "amount"])


def ensure_table(conn: duckdb.DuckDBPyConnection, table: str) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS "{table}" (
            ts_code VARCHAR,
            trade_time VARCHAR,
            open DOUBLE,
            close DOUBLE,
            high DOUBLE,
            low DOUBLE,
            vol BIGINT,
            amount DOUBLE
        )
        """
    )


def insert_dataframe_by_name(conn: duckdb.DuckDBPyConnection, table: str, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    conn.register("tmp_stk_mins_df", df)
    try:
        conn.execute(
            f"""
            MERGE INTO "{table}" AS t
            USING (
                SELECT ts_code, trade_time, open, close, high, low, vol, amount
                FROM (
                    SELECT
                        ts_code,
                        trade_time,
                        open,
                        close,
                        high,
                        low,
                        vol,
                        amount,
                        ROW_NUMBER() OVER (
                            PARTITION BY ts_code, trade_time
                            ORDER BY trade_time DESC
                        ) AS rn
                    FROM tmp_stk_mins_df
                ) s
                WHERE rn = 1
            ) AS src
            ON t.ts_code = src.ts_code
               AND t.trade_time = src.trade_time
            WHEN MATCHED THEN UPDATE SET
                open = src.open,
                close = src.close,
                high = src.high,
                low = src.low,
                vol = src.vol,
                amount = src.amount
            WHEN NOT MATCHED THEN INSERT (
                ts_code,
                trade_time,
                open,
                close,
                high,
                low,
                vol,
                amount
            ) VALUES (
                src.ts_code,
                src.trade_time,
                src.open,
                src.close,
                src.high,
                src.low,
                src.vol,
                src.amount
            )
            """
        )
        return len(df)
    finally:
        conn.unregister("tmp_stk_mins_df")


def setup_logging(log_file: str, log_level: str, console_log: bool) -> None:
    log_path = Path(log_file).expanduser()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    level_name = log_level.strip().upper()
    level = getattr(logging, level_name, logging.INFO)

    handlers: list[logging.Handler] = [
        RotatingFileHandler(
            filename=str(log_path),
            maxBytes=20 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
    ]
    if console_log:
        handlers.append(logging.StreamHandler())

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=handlers,
    )


async def fetch_one_code_window(
    pro: Any,
    ts_code: str,
    api_range: APIRange,
    freq: str,
    semaphore: asyncio.Semaphore,
    limiter: AsyncRateLimiter,
) -> list[dict[str, Any]]:
    async with semaphore:
        await limiter.acquire()
        df = await asyncio.to_thread(
            pro.stk_mins,
            ts_code=ts_code,
            start_date=api_range.start_date,
            end_date=api_range.end_date,
            freq=freq,
        )
    if df is None or df.empty:
        return []
    return df.to_dict("records")


async def run_sync(cfg: SyncConfig) -> None:
    ts.set_token(cfg.token)
    pro = ts.pro_api()

    logger.info("开始拉取 stock_basic...")
    ts_codes = await asyncio.to_thread(fetch_stock_basic_codes, pro, cfg.list_status, cfg.stock_basic_page_size)
    if not ts_codes:
        raise RuntimeError("stock_basic 未获取到任何 ts_code")
    logger.info("stock_basic 获取完成: %s 个 ts_code", len(ts_codes))

    start_step, end_step = stk_mins_step_span(cfg.start_date, cfg.end_date)
    windows = generate_30d_half_open_windows(start_step, end_step)
    logger.info("时间窗数量: %s", len(windows))

    db_path = Path(cfg.duckdb_path).expanduser()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    write_lock = asyncio.Lock()
    try:
        if cfg.init_table:
            ensure_table(conn, cfg.table)

        limiter = AsyncRateLimiter(rate_per_minute=cfg.rate_per_minute)
        semaphore = asyncio.Semaphore(cfg.concurrency)
        total_tasks = len(windows) * len(ts_codes)
        progress = ProgressReporter(total=total_tasks, report_every_seconds=cfg.progress_report_seconds)
        logger.info("总任务量: %s (windows=%s x symbols=%s)", total_tasks, len(windows), len(ts_codes))

        total_inserted = 0
        total_fetched = 0

        for idx, window in enumerate(windows, start=1):
            api_range = api_range_from_half_open_window(window)
            logger.info(
                "[window %s/%s] %s ~ %s | symbols=%s",
                idx,
                len(windows),
                api_range.start_date,
                api_range.end_date,
                len(ts_codes),
            )
            for code_chunk in chunked(ts_codes, cfg.chunk_size):
                tasks = [
                    asyncio.create_task(
                        fetch_one_code_window(
                            pro=pro,
                            ts_code=code,
                            api_range=api_range,
                            freq=cfg.freq,
                            semaphore=semaphore,
                            limiter=limiter,
                        )
                    )
                    for code in code_chunk
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                merged_records: list[dict[str, Any]] = []
                err_count = 0
                for result in results:
                    if isinstance(result, Exception):
                        err_count += 1
                        continue
                    merged_records.extend(result)
                success_count = len(code_chunk) - err_count
                if err_count > 0:
                    logger.warning("窗口 %s 有 %s 个请求失败（已跳过）", idx, err_count)

                total_fetched += len(merged_records)
                if not merged_records:
                    progress.advance(
                        len(code_chunk),
                        success=success_count,
                        failed=err_count,
                        fetched_rows=0,
                        inserted_rows=0,
                    )
                    continue

                df = await asyncio.to_thread(to_rows_dataframe, merged_records)
                async with write_lock:
                    inserted = await asyncio.to_thread(insert_dataframe_by_name, conn, cfg.table, df)
                total_inserted += inserted
                progress.advance(
                    len(code_chunk),
                    success=success_count,
                    failed=err_count,
                    fetched_rows=len(merged_records),
                    inserted_rows=inserted,
                )

            logger.info(
                "[window %s/%s] 累计 fetched=%s, inserted=%s",
                idx,
                len(windows),
                total_fetched,
                total_inserted,
            )
            progress.maybe_report(force=True)

        progress.maybe_report(force=True)
        logger.info(
            "完成: fetched=%s, inserted=%s, table=%s, db=%s",
            total_fetched,
            total_inserted,
            cfg.table,
            db_path,
        )
    finally:
        conn.close()


@app.command()
def main(
    duckdb_path: str = typer.Option(..., "--duckdb-path", help="DuckDB 文件路径"),
    table: str = typer.Option("stk_mins", help="目标表名"),
    start_date: str = typer.Option(..., "--start-date", help="起始日期，支持 yyyymmdd 等"),
    end_date: str = typer.Option(..., "--end-date", help="结束日期，支持 yyyymmdd 等"),
    freq: str = typer.Option("1min", help="stk_mins 频率参数"),
    window_freq: str = typer.Option("30D", "--window-freq", help="时间窗步长，当前仅支持 30D"),
    list_status: str = typer.Option("L", "--list-status", help="stock_basic 的 list_status"),
    concurrency: int = typer.Option(16, help="并发请求数"),
    rate_per_minute: int = typer.Option(450, "--rate-per-minute", help="全局限流（次/分钟）"),
    chunk_size: int = typer.Option(200, "--chunk-size", help="单批创建任务数量"),
    stock_basic_page_size: int = typer.Option(5000, "--stock-basic-page-size", help="stock_basic 分页大小"),
    progress_report_seconds: float = typer.Option(10.0, "--progress-report-seconds", help="进度日志间隔秒数"),
    init_table: bool = typer.Option(False, "--init-table", help="若表不存在则按 8 字段建表"),
    token: str = typer.Option("", help="Tushare token，不传则读环境变量 TUSHARE_TOKEN"),
    log_file: str = typer.Option("qdhub/logs/stk_mins_sync.log", "--log-file", help="文件日志路径"),
    log_level: str = typer.Option("INFO", "--log-level", help="日志级别（DEBUG/INFO/WARNING/ERROR）"),
    console_log: bool = typer.Option(True, "--console-log/--no-console-log", help="是否同时输出控制台日志"),
) -> None:
    setup_logging(log_file=log_file, log_level=log_level, console_log=console_log)
    token = (token or os.getenv("TUSHARE_TOKEN", "")).strip()
    if not token:
        logger.error("请通过 --token 或环境变量 TUSHARE_TOKEN 提供 token")
        raise typer.Exit(code=1)

    cfg = SyncConfig(
        token=token,
        duckdb_path=duckdb_path,
        table=table,
        start_date=start_date,
        end_date=end_date,
        freq=freq,
        window_freq=window_freq,
        list_status=list_status,
        concurrency=concurrency,
        rate_per_minute=rate_per_minute,
        chunk_size=chunk_size,
        stock_basic_page_size=stock_basic_page_size,
        progress_report_seconds=progress_report_seconds,
        init_table=init_table,
    )
    logger.info("任务启动: table=%s, duckdb=%s, range=%s~%s", cfg.table, cfg.duckdb_path, cfg.start_date, cfg.end_date)
    try:
        asyncio.run(run_sync(cfg))
    except Exception:
        logger.exception("任务失败")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
