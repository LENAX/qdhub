### 4.6 Workflow 参数模板化

当 Workflow 的参数需要在执行时动态设置时，可以使用参数模板化功能。在 Build 时使用占位符 `${param_name}`，在执行前通过 API 替换为实际值。

#### 4.6.1 基本用法

```go
// 1. Build 时使用占位符
taskBuilder := builder.NewTaskBuilder("FetchData", "获取数据", registry)
taskBuilder = taskBuilder.WithJobFunction("FetchFunc", map[string]interface{}{
    "data_source_id": "${data_source_id}",
    "date_range":     "${date_range}",
})
task, _ := taskBuilder.Build()

wfBuilder := builder.NewWorkflowBuilder("DataWorkflow", "数据工作流")
wfBuilder = wfBuilder.WithTask(task)
wfBuilder = wfBuilder.WithParams(map[string]string{
    "workflow_id": "${workflow_id}",
})
workflow, _ := wfBuilder.Build()

// 2. 执行前替换参数
params := map[string]interface{}{
    "data_source_id": "ds_001",
    "date_range":     "2024-01-01,2024-01-31",
    "workflow_id":    "wf_001",
}
err := workflow.ReplaceParams(params)
if err != nil {
    log.Fatalf("替换参数失败: %v", err)
}

// 3. 提交执行
controller, err := engine.SubmitWorkflow(ctx, workflow)
```

#### 4.6.2 细粒度参数替换

支持多种参数替换方式：

```go
// 方式1：仅替换 Workflow 级别的参数
workflowParams := map[string]interface{}{
    "workflow_id": "wf_001",
}
err := workflow.ReplaceWorkflowParams(workflowParams)

// 方式2：替换指定 Task 的参数（通过 Task 名称）
taskParams := map[string]interface{}{
    "data_source_id": "ds_001",
}
err := workflow.ReplaceTaskParams("FetchData", taskParams)

// 方式3：替换指定 Task 的参数（通过 Task ID）
err := workflow.ReplaceTaskParams(taskID, taskParams)

// 方式4：替换所有 Task 的参数（不替换 Workflow 参数）
err := workflow.ReplaceAllTaskParams(taskParams)

// 方式5：同时替换 Workflow 和所有 Task 的参数（便捷方法）
err := workflow.ReplaceParams(allParams)
```

#### 4.6.3 Task 级别的参数替换

Task 也支持独立的参数替换：

```go
// 创建 Task 时使用占位符
taskBuilder := builder.NewTaskBuilder("ProcessData", "处理数据", registry)
taskBuilder = taskBuilder.WithJobFunction("ProcessFunc", map[string]interface{}{
    "input_file": "${input_file}",
    "output_dir": "${output_dir}",
})
task, _ := taskBuilder.Build()

// 替换 Task 参数
params := map[string]interface{}{
    "input_file": "/path/to/input.csv",
    "output_dir": "/path/to/output",
}
err := task.ReplaceParams(params)
```

#### 4.6.4 占位符格式

- 格式：`${param_name}`
- 占位符名称：不包含 `${}`，例如 `${data_source_id}` 中的 `data_source_id`
- 类型转换：参数值会自动转换为字符串类型存储

#### 4.6.5 错误处理

如果存在未替换的占位符，`ReplaceParams` 方法会返回错误：

```go
err := workflow.ReplaceParams(params)
if err != nil {
    // 错误信息包含未替换的占位符列表
    log.Printf("替换参数失败: %v", err)
}
```

### 4.7 提交 Workflow 执行

```go
import (
    "context"
    "github.com/LENAX/task-engine/pkg/core/engine"
)

// 创建 Engine
eng, err := engine.NewEngine(10, 60, workflowRepo, instanceRepo, taskRepo)
if err != nil {
    log.Fatalf("创建 Engine 失败: %v", err)
}

// 启动 Engine
ctx := context.Background()
if err := eng.Start(ctx); err != nil {
    log.Fatalf("启动 Engine 失败: %v", err)
}
defer eng.Stop()

// 提交 Workflow
controller, err := eng.SubmitWorkflow(ctx, wf)
if err != nil {
    log.Fatalf("提交 Workflow 失败: %v", err)
}

// 等待完成
for {
    status := controller.Status()
    if status == "Success" || status == "Failed" {
        break
    }
    time.Sleep(500 * time.Millisecond)
}
```
