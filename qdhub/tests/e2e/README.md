# E2E 测试说明

本目录包含端到端（E2E）测试，支持两种运行模式：**模拟模式（Mock）**和**真实模式（Real）**。

## 测试模式

### Mock 模式（默认）

Mock 模式使用 `httptest` 进行测试，不需要启动真实的 HTTP 服务器。这是默认模式，适合快速测试和 CI/CD 环境。

```bash
# 使用默认的 mock 模式
go test -tags=e2e ./tests/e2e/...

# 或者显式指定
E2E_MODE=mock go test -tags=e2e ./tests/e2e/...
```

### Real 模式

Real 模式会启动真实的 HTTP 服务器，并通过 HTTP 客户端调用 API。适合验证完整的系统集成。

```bash
# 使用真实模式
E2E_MODE=real go test -tags=e2e ./tests/e2e/...

# 指定自定义 API 地址（可选）
E2E_MODE=real QDHUB_API_URL=http://127.0.0.1:8080 go test -tags=e2e ./tests/e2e/...
```

## 环境变量

- `E2E_MODE`: 测试模式，可选值：`mock`（默认）或 `real`
- `QDHUB_API_URL`: 真实模式下的 API 服务器地址（可选，默认为 `http://127.0.0.1:8080`）
- `QDHUB_TUSHARE_TOKEN`: Tushare API Token（可选，用于需要真实 API 调用的测试）
  - 如果未设置，会尝试使用 `TUSHARE_TOKEN` 环境变量
- `QDHUB_ENCRYPTION_KEY`: 加密密钥（可选，用于 token 加密/解密功能测试）

## 运行测试

### 运行所有 E2E 测试

```bash
go test -tags=e2e ./tests/e2e/...
```

### 运行特定测试

```bash
# Mock 模式
go test -tags=e2e ./tests/e2e/... -run TestE2E_DataSourceWorkflow

# Real 模式
E2E_MODE=real go test -tags=e2e ./tests/e2e/... -run TestE2E_DataSourceWorkflow
```

### 查看详细输出

```bash
go test -tags=e2e ./tests/e2e/... -v
```

## 测试文件说明

- `api_e2e_test.go`: 原有的 E2E 测试（仅 Mock 模式）
- `api_e2e_full_test.go`: 新的完整 E2E 测试（支持 Mock 和 Real 模式）

## 测试覆盖

新的 E2E 测试覆盖以下场景：

1. **数据源工作流** (`TestE2E_DataSourceWorkflow`)
   - 创建、获取、更新、删除数据源
   - Token 管理

2. **数据存储工作流** (`TestE2E_DataStoreWorkflow`)
   - 创建、获取、测试连接、删除数据存储

3. **工作流生命周期** (`TestE2E_WorkflowLifecycle`)
   - 创建、获取、执行、启用/禁用、删除工作流

4. **健康检查** (`TestE2E_HealthCheck`)
   - 验证服务器健康状态

5. **错误处理** (`TestE2E_ErrorHandling`)
   - 404 错误
   - 400 错误（验证失败）

6. **批量操作** (`TestE2E_BatchOperations`)
   - 批量创建数据源

## 注意事项

1. **Real 模式**会自动查找可用端口启动服务器，无需手动配置
2. **Mock 模式**使用内存数据库，测试结束后自动清理
3. **Real 模式**使用临时数据库文件，测试结束后自动清理
4. 所有测试都是独立的，可以并行运行

## 参考

参考 `doc/example/e2e_test.go` 了解更详细的 E2E 测试模式设计。
