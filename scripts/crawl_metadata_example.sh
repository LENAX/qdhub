#!/bin/bash
# QDHub 元数据爬取完整示例脚本
# 使用方法: ./scripts/crawl_metadata_example.sh

set -e  # 遇到错误立即退出

BASE_URL="http://localhost:8080"
DATA_SOURCE_NAME="Tushare"

echo "=========================================="
echo "QDHub 元数据爬取示例"
echo "=========================================="
echo ""

# 检查服务是否运行
echo "📡 检查服务状态..."
if ! curl -s -f "$BASE_URL/api/v1/workflows" > /dev/null 2>&1; then
    echo "❌ 错误: 无法连接到 QDHub 服务 ($BASE_URL)"
    echo "   请确保服务已启动: ./bin/qdhub server"
    exit 1
fi
echo "✅ 服务运行正常"
echo ""

# 步骤 1: 创建数据源
echo "=========================================="
echo "步骤 1: 创建数据源"
echo "=========================================="
echo "正在创建数据源: $DATA_SOURCE_NAME..."

DATA_SOURCE_RESP=$(curl -s -X POST "$BASE_URL/api/v1/datasources" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Tushare",
    "description": "Tushare Pro Data Source",
    "base_url": "http://api.tushare.pro",
    "doc_url": "https://tushare.pro/document/2"
  }')

# 检查响应
if echo "$DATA_SOURCE_RESP" | jq -e '.code' > /dev/null 2>&1; then
    CODE=$(echo "$DATA_SOURCE_RESP" | jq -r '.code')
    if [ "$CODE" != "201" ] && [ "$CODE" != "200" ]; then
        echo "❌ 创建数据源失败:"
        echo "$DATA_SOURCE_RESP" | jq '.'
        exit 1
    fi
else
    echo "❌ 无效的响应格式:"
    echo "$DATA_SOURCE_RESP"
    exit 1
fi

DATA_SOURCE_ID=$(echo "$DATA_SOURCE_RESP" | jq -r '.data.id // .data[0].id // empty')
if [ -z "$DATA_SOURCE_ID" ]; then
    echo "❌ 无法获取数据源 ID"
    echo "响应: $DATA_SOURCE_RESP"
    exit 1
fi

echo "✅ 数据源创建成功"
echo "   数据源 ID: $DATA_SOURCE_ID"
echo ""

# 步骤 2: 设置 Token（可选）
echo "=========================================="
echo "步骤 2: 设置 Token（可选）"
echo "=========================================="
read -p "是否设置 Tushare Token? (y/n): " SET_TOKEN
if [ "$SET_TOKEN" = "y" ] || [ "$SET_TOKEN" = "Y" ]; then
    read -sp "请输入 Tushare Token: " TOKEN
    echo ""
    
    TOKEN_RESP=$(curl -s -X POST "$BASE_URL/api/v1/datasources/$DATA_SOURCE_ID/token" \
      -H "Content-Type: application/json" \
      -d "{\"token\": \"$TOKEN\"}")
    
    if echo "$TOKEN_RESP" | jq -e '.code == 200' > /dev/null 2>&1; then
        echo "✅ Token 设置成功"
    else
        echo "⚠️  Token 设置可能失败，但继续执行..."
        echo "$TOKEN_RESP" | jq '.'
    fi
else
    echo "⏭️  跳过 Token 设置"
fi
echo ""

# 步骤 3: 执行元数据爬取
echo "=========================================="
echo "步骤 3: 执行元数据爬取"
echo "=========================================="
echo "正在提交元数据爬取工作流..."

INSTANCE_RESP=$(curl -s -X POST "$BASE_URL/api/v1/workflows/built-in/metadata_crawl/execute" \
  -H "Content-Type: application/json" \
  -d "{
    \"trigger_type\": \"manual\",
    \"trigger_params\": {
      \"data_source_id\": \"$DATA_SOURCE_ID\",
      \"data_source_name\": \"tushare\",
      \"max_api_crawl\": 0
    }
  }")

# 检查响应
if echo "$INSTANCE_RESP" | jq -e '.code' > /dev/null 2>&1; then
    CODE=$(echo "$INSTANCE_RESP" | jq -r '.code')
    if [ "$CODE" != "200" ]; then
        echo "❌ 提交工作流失败:"
        echo "$INSTANCE_RESP" | jq '.'
        exit 1
    fi
else
    echo "❌ 无效的响应格式:"
    echo "$INSTANCE_RESP"
    exit 1
fi

INSTANCE_ID=$(echo "$INSTANCE_RESP" | jq -r '.data.instance_id // .data[0].instance_id // empty')
if [ -z "$INSTANCE_ID" ]; then
    echo "❌ 无法获取工作流实例 ID"
    echo "响应: $INSTANCE_RESP"
    exit 1
fi

echo "✅ 工作流已提交"
echo "   实例 ID: $INSTANCE_ID"
echo ""

# 步骤 4: 监控执行状态
echo "=========================================="
echo "步骤 4: 监控执行状态"
echo "=========================================="
echo "正在监控工作流执行..."
echo ""

MAX_WAIT=300  # 最大等待时间（秒）
ELAPSED=0
LAST_PROGRESS=0

while [ $ELAPSED -lt $MAX_WAIT ]; do
    STATUS_RESP=$(curl -s "$BASE_URL/api/v1/instances/$INSTANCE_ID/progress")
    
    if ! echo "$STATUS_RESP" | jq -e '.data' > /dev/null 2>&1; then
        echo "⚠️  无法获取状态，等待中..."
        sleep 5
        ELAPSED=$((ELAPSED + 5))
        continue
    fi
    
    STATUS=$(echo "$STATUS_RESP" | jq -r '.data.status // "Unknown"')
    PROGRESS=$(echo "$STATUS_RESP" | jq -r '.data.progress // 0')
    COMPLETED=$(echo "$STATUS_RESP" | jq -r '.data.completed_task // 0')
    TOTAL=$(echo "$STATUS_RESP" | jq -r '.data.task_count // 0')
    
    # 只在进度变化时输出
    if [ "$(printf "%.0f" "$PROGRESS")" != "$(printf "%.0f" "$LAST_PROGRESS")" ]; then
        echo "[$(date +%H:%M:%S)] 状态: $STATUS | 进度: ${PROGRESS}% | 任务: $COMPLETED/$TOTAL"
        LAST_PROGRESS=$PROGRESS
    fi
    
    if [ "$STATUS" = "Success" ]; then
        echo ""
        echo "✅ 工作流执行成功！"
        break
    elif [ "$STATUS" = "Failed" ]; then
        echo ""
        echo "❌ 工作流执行失败"
        ERROR_MSG=$(echo "$STATUS_RESP" | jq -r '.data.error_message // "未知错误"')
        echo "   错误信息: $ERROR_MSG"
        exit 1
    fi
    
    sleep 5
    ELAPSED=$((ELAPSED + 5))
done

if [ $ELAPSED -ge $MAX_WAIT ]; then
    echo ""
    echo "⚠️  等待超时（${MAX_WAIT}秒），请手动检查状态"
    echo "   查看状态: curl $BASE_URL/api/v1/instances/$INSTANCE_ID/progress"
fi
echo ""

# 步骤 5: 查看爬取的 API
echo "=========================================="
echo "步骤 5: 查看爬取的 API"
echo "=========================================="
echo "正在获取 API 列表..."

APIS_RESP=$(curl -s "$BASE_URL/api/v1/datasources/$DATA_SOURCE_ID/apis")

if echo "$APIS_RESP" | jq -e '.data' > /dev/null 2>&1; then
    API_COUNT=$(echo "$APIS_RESP" | jq '.data | length')
    echo "✅ 找到 $API_COUNT 个 API"
    echo ""
    echo "API 列表:"
    echo "$APIS_RESP" | jq -r '.data[] | "  - \(.name): \(.display_name // .name)"' | head -20
    
    if [ "$API_COUNT" -gt 20 ]; then
        echo "  ... (还有 $((API_COUNT - 20)) 个 API)"
    fi
else
    echo "⚠️  无法获取 API 列表"
    echo "响应: $APIS_RESP"
fi
echo ""

# 完成
echo "=========================================="
echo "✅ 完成！"
echo "=========================================="
echo ""
echo "数据源 ID: $DATA_SOURCE_ID"
echo "工作流实例 ID: $INSTANCE_ID"
echo ""
echo "后续操作:"
echo "  1. 查看所有 API: curl $BASE_URL/api/v1/datasources/$DATA_SOURCE_ID/apis"
echo "  2. 创建数据表: 使用 create_tables 工作流"
echo "  3. 同步数据: 使用 batch_data_sync 或 realtime_data_sync 工作流"
echo ""
