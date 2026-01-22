#!/usr/bin/env python3
"""
生成 Tushare API 同步策略的 SQL 迁移脚本
根据 api_metadata 表的 request_params 分析每个 API 的策略配置
"""

import json
import sqlite3
import sys
from pathlib import Path

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "qdhub" / "tests" / "e2e" / "data" / "e2e_app.db"

# 已存在的策略（从迁移脚本中提取）
EXISTING_STRATEGIES = {
    'trade_cal', 'stock_basic', 'namechange', 'index_basic', 'hs_const', 'stk_limit',
    'daily', 'weekly', 'monthly', 'daily_basic', 'adj_factor', 'top_list', 'top_inst',
    'margin', 'margin_detail', 'block_trade', 'index_daily', 'index_weight',
    'moneyflow_hsgt', 'moneyflow_ind_ths', 'moneyflow_cnt_ths', 'moneyflow_mkt_dc',
    'moneyflow_ind_dc', 'moneyflow', 'moneyflow_ths', 'moneyflow_dc',
    'hsgt_top10', 'ggt_top10', 'limit_list_d',
    'ths_index', 'ths_daily', 'ths_member',
    'kpl_list', 'kpl_concept', 'kpl_concept_cons',
    'income', 'balancesheet', 'cashflow', 'fina_indicator', 'fina_mainbz'
}


def analyze_api_strategy(api_name, request_params):
    """
    根据 request_params 分析 API 的同步策略
    
    返回: (preferred_param, support_date_range, required_params, dependencies, description)
    """
    if not request_params:
        return 'none', 0, None, None, f'{api_name} - 无参数信息'
    
    try:
        params = json.loads(request_params) if isinstance(request_params, str) else request_params
    except:
        return 'none', 0, None, None, f'{api_name} - 参数解析失败'
    
    # 分析必填参数
    required_param_names = [p['name'] for p in params if p.get('required', False)]
    
    # 检查是否支持日期范围
    has_start_date = any(p['name'] == 'start_date' for p in params)
    has_end_date = any(p['name'] == 'end_date' for p in params)
    has_trade_date = any(p['name'] == 'trade_date' for p in params)
    has_ts_code = any(p['name'] == 'ts_code' for p in params)
    
    support_date_range = 1 if (has_start_date and has_end_date) else 0
    
    # 判断 preferred_param
    if not required_param_names:
        # 无必填参数
        if has_trade_date and (has_start_date or has_end_date):
            preferred_param = 'trade_date'
            dependencies = '["FetchTradeCal"]'
        else:
            preferred_param = 'none'
            dependencies = None
    elif 'ts_code' in required_param_names:
        # ts_code 必填
        preferred_param = 'ts_code'
        dependencies = '["FetchStockBasic"]'
    elif 'trade_date' in required_param_names or (has_trade_date and not has_ts_code):
        # trade_date 相关
        preferred_param = 'trade_date'
        dependencies = '["FetchTradeCal"]'
    elif any(p in required_param_names for p in ['date', 'month', 'q']):
        # 日期/月份/季度必填
        preferred_param = 'trade_date'
        dependencies = '["FetchTradeCal"]'
    else:
        # 其他必填参数
        preferred_param = 'none'
        dependencies = None
    
    # 提取必填参数（除了 preferred_param）
    other_required = [p for p in required_param_names if p not in [preferred_param, 'ts_code', 'trade_date']]
    required_params = json.dumps(other_required) if other_required else None
    
    # 生成描述
    desc_parts = []
    if required_param_names:
        desc_parts.append(f"必填: {', '.join(required_param_names)}")
    if has_trade_date:
        desc_parts.append("支持 trade_date")
    if support_date_range:
        desc_parts.append("支持 start_date+end_date")
    description = f'{api_name} - {" | ".join(desc_parts) if desc_parts else "无必填参数"}'
    
    return preferred_param, support_date_range, required_params, dependencies, description


def generate_sql_insert(api_name, preferred_param, support_date_range, required_params, dependencies, description):
    """生成 SQL INSERT 语句"""
    required_params_str = f"'{required_params}'" if required_params else "NULL"
    dependencies_str = f"'{dependencies}'" if dependencies else "NULL"
    if description:
        escaped_desc = description.replace("'", "''")
        description_str = f"'{escaped_desc}'"
    else:
        description_str = "NULL"
    
    return f"""INSERT OR REPLACE INTO api_sync_strategies (id, data_source_id, api_name, preferred_param, support_date_range, required_params, dependencies, description)
SELECT 
    lower(hex(randomblob(16))),
    ds.id,
    '{api_name}',
    '{preferred_param}',
    {support_date_range},
    {required_params_str},
    {dependencies_str},
    {description_str}
FROM data_sources ds WHERE LOWER(ds.name) = 'tushare';

"""


def main():
    """主函数"""
    if not DB_PATH.exists():
        print(f"错误: 数据库文件不存在: {DB_PATH}", file=sys.stderr)
        sys.exit(1)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 获取所有 tushare API
    cursor.execute("""
        SELECT name, request_params 
        FROM api_metadata 
        WHERE data_source_id IN (
            SELECT id FROM data_sources WHERE LOWER(name) = 'tushare'
        )
        ORDER BY name
    """)
    
    apis = cursor.fetchall()
    conn.close()
    
    print(f"-- ==================== 补充的 Tushare API 同步策略 ====================")
    print(f"-- 共 {len(apis)} 个 API，已存在 {len(EXISTING_STRATEGIES)} 个策略")
    print(f"-- 需要补充 {len(apis) - len(EXISTING_STRATEGIES)} 个策略\n")
    
    sql_statements = []
    missing_apis = []
    
    for api_name, request_params in apis:
        if api_name in EXISTING_STRATEGIES:
            continue
        
        preferred_param, support_date_range, required_params, dependencies, description = \
            analyze_api_strategy(api_name, request_params)
        
        sql = generate_sql_insert(api_name, preferred_param, support_date_range, 
                                 required_params, dependencies, description)
        sql_statements.append(sql)
        missing_apis.append(api_name)
    
    # 按 preferred_param 分组输出
    print("-- ========== 按 preferred_param 分组 ==========\n")
    
    # 分组
    groups = {
        'none': [],
        'trade_date': [],
        'ts_code': []
    }
    
    for api_name, sql in zip(missing_apis, sql_statements):
        # 从 SQL 中提取 preferred_param
        if "'none'" in sql:
            groups['none'].append((api_name, sql))
        elif "'trade_date'" in sql:
            groups['trade_date'].append((api_name, sql))
        elif "'ts_code'" in sql:
            groups['ts_code'].append((api_name, sql))
    
    # 输出分组
    for group_name, items in groups.items():
        if items:
            print(f"-- ========== preferred_param: {group_name} ==========")
            for api_name, sql in items:
                print(sql)
            print()
    
    print(f"-- 共生成 {len(sql_statements)} 个策略配置")


if __name__ == '__main__':
    main()
