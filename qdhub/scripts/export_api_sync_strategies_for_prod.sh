#!/usr/bin/env bash
# 从本地 qdhub.db 导出 api_sync_strategies，生成可在线上执行的 SQL。
# data_source_id 使用子查询 (SELECT id FROM data_sources WHERE LOWER(name) = 'tushare' LIMIT 1)，线上执行时自动用线上的 Tushare id。
#
# 用法:
#   1) 在项目根目录或 qdhub 目录执行:
#      cd qdhub && ./scripts/export_api_sync_strategies_for_prod.sh [本地DB路径]
#   2) 将输出重定向到文件: ./scripts/export_api_sync_strategies_for_prod.sh > api_sync_strategies_prod.sql
#   3) 在线上执行: sqlite3 /path/to/prod/qdhub.db < api_sync_strategies_prod.sql

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QDHUB_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOCAL_DB="${1:-$QDHUB_DIR/data/qdhub.db}"

if [[ ! -f "$LOCAL_DB" ]]; then
  echo "Usage: $0 [local_db_path]" >&2
  echo "  local_db_path default: $LOCAL_DB" >&2
  exit 1
fi

DS_SUBQUERY="(SELECT id FROM data_sources WHERE LOWER(name) = 'tushare' LIMIT 1)"

echo "BEGIN;"
echo "DELETE FROM api_sync_strategies;"

# 使用 TAB 分隔，避免数据中的 | 或逗号破坏列对齐
sqlite3 -batch -separator $'\t' "$LOCAL_DB" "
SELECT
  COALESCE(quote(id), 'NULL'),
  COALESCE(quote(api_name), 'NULL'),
  COALESCE(quote(preferred_param), 'NULL'),
  COALESCE(support_date_range, 0),
  COALESCE(quote(required_params), 'NULL'),
  COALESCE(quote(dependencies), 'NULL'),
  COALESCE(quote(description), 'NULL'),
  COALESCE(quote(created_at), 'NULL'),
  COALESCE(quote(updated_at), 'NULL'),
  COALESCE(quote(fixed_params), 'NULL'),
  COALESCE(quote(fixed_param_keys), 'NULL'),
  COALESCE(realtime_ts_code_chunk_size, 0),
  COALESCE(quote(realtime_ts_code_format), 'NULL'),
  COALESCE(quote(iterate_params), 'NULL')
FROM api_sync_strategies;
" | while IFS=$'\t' read -r id api_name preferred_param support_date_range required_params dependencies description created_at updated_at fixed_params fixed_param_keys realtime_ts_code_chunk_size realtime_ts_code_format iterate_params; do
  echo "INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description, created_at, updated_at, fixed_params, fixed_param_keys, realtime_ts_code_chunk_size, realtime_ts_code_format, iterate_params) VALUES ($id, $DS_SUBQUERY, $api_name, $preferred_param, $support_date_range, $required_params, $dependencies, $description, $created_at, $updated_at, $fixed_params, $fixed_param_keys, $realtime_ts_code_chunk_size, $realtime_ts_code_format, $iterate_params);"
done

echo "COMMIT;"
