# 紧急修复：线上 api_sync_strategies 与 Casbin 权限

## 1. 修复 admin 无法修改 api-sync-strategy（Casbin 权限）

在**线上**库执行（sqlite3）：

```bash
sqlite3 /path/to/prod/qdhub.db < scripts/emergency_fix_casbin_api_sync_strategies.sql
```

## 2. 用本地 api_sync_strategies 替换线上

**步骤一：在本地**导出 SQL（假设本地库在 `qdhub/data/qdhub.db`）：

```bash
cd qdhub
./scripts/export_api_sync_strategies_for_prod.sh > api_sync_strategies_prod.sql
```

或指定本地 DB 路径：

```bash
./scripts/export_api_sync_strategies_for_prod.sh /path/to/local/qdhub.db > api_sync_strategies_prod.sql
```

**步骤二：**把生成的 `api_sync_strategies_prod.sql` 拷到线上。

**步骤三：在线上**执行（会先 `DELETE` 再 `INSERT`，`data_source_id` 自动用线上的 Tushare 数据源 id）：

```bash
sqlite3 /path/to/prod/qdhub.db < api_sync_strategies_prod.sql
```

注意：线上必须存在名为 `Tushare`（不区分大小写）的 data source，否则子查询无结果会导致插入失败。
