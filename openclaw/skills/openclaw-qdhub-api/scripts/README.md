# QDHub 脚本说明

供 OpenClaw 或本地调用的 Python 命令行工具，通过环境变量认证，避免在技能或脚本中硬编码账号密码。

与 **彭道富龙头**技能联用时：先用本脚本拉取接口 JSON，再将事实写入 `DragonHeadScreening.qdhub_evidence`（见上级目录 `SKILL.md` §1.1 与 `openclaw/skills/peng-daofu-dragon-head/`）。

## 依赖

- Python 3.10+（建议 3.15）
- `pip install -r requirements.txt`（主要依赖 `httpx`；若已安装 httpx CLI 则通常已具备）

## 环境变量

| 变量 | 必填 | 说明 |
|------|------|------|
| `QDHUB_USERNAME` | login 时必填 | 登录用户名 |
| `QDHUB_PASSWORD` | login 时必填 | 登录密码 |
| `QDHUB_BASE_URL` | 否 | 默认 `https://qdhub.quantrade.team` |
| `QDHUB_ACCESS_TOKEN` | 否 | 若已设置则直接用于 get/post，无需先 login |
| `QDHUB_TOKEN_FILE` | 否 | token 持久化文件路径；login 时写入，get/post 时未设置 token 则从此文件读取 |

## 用法

```bash
# 登录并输出可导出的环境变量（便于 eval 更新当前 shell）
eval $(python qdhub_cli.py login)

# 或指定 token 文件，后续 get/post 自动从文件读 token
export QDHUB_TOKEN_FILE=~/.config/openclaw/qdhub_token
export QDHUB_USERNAME=guest
export QDHUB_PASSWORD=your_password
python qdhub_cli.py login

# GET 请求（query 参数用 key=value 依次传入）
python qdhub_cli.py get /api/v1/analysis/trade-cal start_date=20250101 end_date=20251231

# 指数/行业分类与成分
python qdhub_cli.py get /api/v1/analysis/index-sectors src=SW2021 level=L1 limit=50
python qdhub_cli.py get /api/v1/analysis/index-sector-members index_code=000300.SH trade_date=20250301 limit=100

# POST 请求（JSON body 用 --body）
python qdhub_cli.py post /api/v1/analysis/custom-query/query --body '{"sql":"SELECT * FROM trade_cal LIMIT 5","max_rows":10,"timeout_seconds":30}'
```

输出均为 JSON，便于管道处理或由 OpenClaw 解析。
