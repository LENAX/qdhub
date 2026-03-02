#!/bin/bash
# 运行 E2E 测试的包装脚本
# 解决 Go 1.24.2 在解析包路径时扫描所有子目录的问题

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# 进入 tests/e2e 目录运行测试（避免扫描 qdhub 目录下的 bin 等目录）
cd tests/e2e

# 临时移动可能干扰的目录
TEMP_DIR=$(mktemp -d)
trap "cd '$SCRIPT_DIR' && rm -rf '$TEMP_DIR'" EXIT

if [ -d "data" ]; then
    mv data "$TEMP_DIR/e2e_data" 2>/dev/null || true
    RESTORE_DATA=true
fi
if [ -d "logs" ]; then
    mv logs "$TEMP_DIR/e2e_logs" 2>/dev/null || true
    RESTORE_LOGS=true
fi

# 运行测试
go test -tags e2e "$@" .

# 恢复目录
cd "$SCRIPT_DIR/tests/e2e"
if [ "$RESTORE_DATA" = "true" ] && [ -d "$TEMP_DIR/e2e_data" ]; then
    mv "$TEMP_DIR/e2e_data" data 2>/dev/null || true
fi
if [ "$RESTORE_LOGS" = "true" ] && [ -d "$TEMP_DIR/e2e_logs" ]; then
    mv "$TEMP_DIR/e2e_logs" logs 2>/dev/null || true
fi
