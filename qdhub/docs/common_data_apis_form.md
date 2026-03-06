# 数据源「公共数据 API」表单对接说明

用于在数据源设置/编辑表单中增加「公共数据 API」勾选项，勾选的 API 在同步时会走缓存优先（Cache → DuckDB → API），减少重复请求。

## 接口契约

- **Base URL**: `/api/v1`（需鉴权）

### 1. 获取数据源详情（含当前已选公共 API）

```
GET /datasources/:id
```

**响应**（节选）:
```json
{
  "code": 0,
  "data": {
    "id": "xxx",
    "name": "tushare",
    "common_data_apis": ["trade_cal", "stock_basic"]
  }
}
```

- `common_data_apis`: 当前已设为「公共数据」的 API 名称数组；未设置时为 `[]` 或不存在该字段。

### 2. 获取可勾选的 API 名称列表

```
GET /datasources/:id/api-names
```

**响应**:
```json
{
  "code": 0,
  "data": {
    "api_names": ["trade_cal", "stock_basic", "daily", "adj_factor", ...]
  }
}
```

- 用于渲染表单中的勾选项（复选框）。勾选状态由「当前已选」与 `common_data_apis` 对比得出。

### 3. 保存公共数据 API 设置

```
PUT /datasources/:id/common-data-apis
Content-Type: application/json

{
  "common_data_apis": ["trade_cal", "stock_basic"]
}
```

- 传入选中的 API 名称数组；传空数组 `[]` 表示清空。

## 前端表单流程建议

1. **进入表单**（如数据源详情/设置页）  
   - 请求 `GET /datasources/:id` → 得到 `common_data_apis`。  
   - 请求 `GET /datasources/:id/api-names` → 得到 `api_names`。

2. **渲染勾选项**  
   - 遍历 `api_names`，每个 API 一个复选框。  
   - 若 `common_data_apis.indexOf(apiName) !== -1`，则该复选框默认勾选。

3. **提交保存**  
   - 收集当前勾选的 API 名称组成数组 `selected`。  
   - 请求 `PUT /datasources/:id/common-data-apis`，body: `{ "common_data_apis": selected }`。

## 简单 HTML + JS 示例（仅作接口联调参考）

```html
<div id="form">
  <p>公共数据 API（勾选后同步时优先走缓存）：</p>
  <div id="checkboxes"></div>
  <button id="save">保存</button>
</div>

<script>
const dataSourceId = 'YOUR_DATASOURCE_ID';
const token = 'YOUR_JWT';

async function load() {
  const [dsRes, namesRes] = await Promise.all([
    fetch(`/api/v1/datasources/${dataSourceId}`, { headers: { Authorization: 'Bearer ' + token } }),
    fetch(`/api/v1/datasources/${dataSourceId}/api-names`, { headers: { Authorization: 'Bearer ' + token } })
  ]);
  const ds = (await dsRes.json()).data;
  const { api_names } = (await namesRes.json()).data;
  const common = ds.common_data_apis || [];

  const container = document.getElementById('checkboxes');
  container.innerHTML = api_names.map(name => `
    <label><input type="checkbox" data-name="${name}" ${common.includes(name) ? 'checked' : ''}> ${name}</label><br>
  `).join('');
}

document.getElementById('save').onclick = async () => {
  const selected = [...document.querySelectorAll('#checkboxes input:checked')].map(el => el.dataset.name);
  const res = await fetch(`/api/v1/datasources/${dataSourceId}/common-data-apis`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token },
    body: JSON.stringify({ common_data_apis: selected })
  });
  if (res.ok) alert('已保存');
  else alert('保存失败');
};

load();
</script>
```

## 说明

- 若数据源尚未导入 API 元数据，`api_names` 可能为空，此时不展示勾选项或提示先刷新/导入元数据。  
- 常见可设为公共数据的 API（如 Tushare）：`trade_cal`、`stock_basic`、`index_basic` 等全量、变更较少的基础接口。
