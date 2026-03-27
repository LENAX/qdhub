#!/usr/bin/env zsh
set -euo pipefail

start_date=""
end_date=""
SCRIPT_NAME="${0:t}"

usage() {
  cat <<EOF
用法：
  ${SCRIPT_NAME} --start-date YYYYMMDD --end-date YYYYMMDD

如果不传日期，会在运行时交互输入。日期格式必须是 yyyymmdd（例如 20170701）。

要求：
  环境变量 TUSHARE_TOKEN 必须已设置（脚本会启动 tmux 并在其中使用该 token）
EOF
}

validate_yyyymmdd() {
  local v="$1"
  if [[ ! "$v" =~ ^[0-9]{8}$ ]]; then
    echo "日期格式错误：'$v'，期望 yyyymmdd，例如 20170701" >&2
    exit 1
  fi
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --start-date)
        start_date="${2:-}"
        shift 2
        ;;
      --end-date)
        end_date="${2:-}"
        shift 2
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        echo "未知参数：$1" >&2
        usage >&2
        exit 1
        ;;
    esac
  done
}

session_name="stkmins"
duckdb_path="/mnt/data/qdhub/data/ts_stk_min.duckdb"
log_file_name="stk_mins_sync1.log"

SCRIPT_DIR="${0:A:h}"     # .../qdhub/scripts
REPO_ROOT="${SCRIPT_DIR:h:h}" # .../qdhub

parse_args "$@"

if [[ -z "${start_date}" ]]; then
  if [[ ! -t 0 ]]; then
    echo "缺少 --start-date，且当前不是交互终端，无法等待输入" >&2
    usage >&2
    exit 1
  fi
  read -r "start_date?请输入 start_date (yyyymmdd)："
fi

if [[ -z "${end_date}" ]]; then
  if [[ ! -t 0 ]]; then
    echo "缺少 --end-date，且当前不是交互终端，无法等待输入" >&2
    usage >&2
    exit 1
  fi
  read -r "end_date?请输入 end_date (yyyymmdd)："
fi

validate_yyyymmdd "${start_date}"
validate_yyyymmdd "${end_date}"

if [[ "${end_date}" < "${start_date}" ]]; then
  echo "end_date 必须 >= start_date（当前：${start_date} -> ${end_date}）" >&2
  exit 1
fi

# 要求：环境变量 TUSHARE_TOKEN 已设置（在 tmux 里使用）
: "${TUSHARE_TOKEN:?请先在当前 shell 设置环境变量 TUSHARE_TOKEN}"

log_file="${REPO_ROOT}/qdhub/logs/${log_file_name}"
mkdir -p "${REPO_ROOT}/qdhub/logs"

if tmux has-session -t "${session_name}" 2>/dev/null; then
  echo "tmux session '${session_name}' 已存在：请先关闭或换一个 session_name"
  exit 1
fi

quoted_token="${(q)TUSHARE_TOKEN}"
quoted_repo_root="${(q)REPO_ROOT}"
quoted_duckdb_path="${(q)duckdb_path}"
quoted_log_file="${(q)log_file}"
quoted_start_date="${(q)start_date}"
quoted_end_date="${(q)end_date}"

tmux new -d -s "${session_name}" \
  "export TUSHARE_TOKEN=${quoted_token}; cd ${quoted_repo_root} && uv run qdhub/scripts/tushare_sync_stk_mins.py \
  --duckdb-path ${quoted_duckdb_path} \
  --start-date ${quoted_start_date} \
  --end-date ${quoted_end_date} \
  --log-file ${quoted_log_file} \
  --log-level INFO \
  --console-log"

echo "已启动：tmux session='${session_name}'，日志='${log_file}'"

