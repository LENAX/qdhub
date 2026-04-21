# 脚本说明

## 依赖

在项目根执行：

```bash
pip install -r openclaw/requirements.txt
```

## validate_screening_json.py

校验并 pretty-print `DragonHeadScreening` 兼容的 JSON。

```bash
cd /path/to/qdhub
PYTHONPATH=. python3 openclaw/skills/peng-daofu-dragon-head/scripts/validate_screening_json.py screening.json
```
