"""
Jupyter Server 配置（nginx 反向代理：路径前缀或独立子域名均可）。

挂载：docker-compose.jupyter.yml 中挂到
/home/jovyan/.jupyter/jupyter_server_config.py

环境变量（可选）：
- JUPYTER_BASE_URL：对外 URL 前缀。默认 "/jupyter/"（与 qdhub.conf 路径反代一致）；
  独立子域名根路径时设为 "/"（例如 export JUPYTER_BASE_URL=/）
- JUPYTER_PASSWORD：明文密码，启动时哈希（勿提交到 git；生产可用 compose secrets）
- JUPYTER_PASSWORD_HASH：已由 `jupyter server password hash` 或
  `python -c "from jupyter_server.auth.security import passwd; print(passwd())"` 生成的哈希字符串；
  若同时设置，优先使用 JUPYTER_PASSWORD_HASH

启用密码登录时须写入 PasswordIdentityProvider.hashed_password（Jupyter Server 2.0+）；
勿再单独设置 JUPYTER_TOKEN（非空时环境变量会优先于「无 token」逻辑）。
改密码后请强制重建容器：docker compose ... up -d --force-recreate
"""

from __future__ import annotations

import os

from jupyter_server.auth.security import passwd
from jupyter_server.serverapp import ServerApp  # noqa: F401

c = get_config()  # noqa: F821  # Jupyter 配置入口，运行时由 traitlets 注入


def _normalize_base_url(raw: str) -> str:
    s = (raw or "/").strip()
    if not s.startswith("/"):
        s = "/" + s
    if len(s) > 1 and not s.endswith("/"):
        s = s + "/"
    return s


# 环境变量存在但为空时（compose 常见）须回退默认，否则会误用根路径 "/"
_base_raw = os.environ.get("JUPYTER_BASE_URL", "/jupyter/")
if isinstance(_base_raw, str) and not _base_raw.strip():
    _base_raw = "/jupyter/"
c.ServerApp.base_url = _normalize_base_url(_base_raw)

c.ServerApp.trust_xheaders = True
# jupyter-server 2.17+ 已移除 ServerApp.preferred_url_scheme；由 trust_xheaders + 反代 X-Forwarded-Proto 即可

c.ServerApp.ip = "0.0.0.0"
c.ServerApp.port = 8888
c.ServerApp.open_browser = False

_pw_hash = os.environ.get("JUPYTER_PASSWORD_HASH", "").strip()
_pw_plain = (os.environ.get("JUPYTER_PASSWORD") or "").strip()

# Jupyter Server 2.x：仅用已弃用的 ServerApp.password 不会同步到 PasswordIdentityProvider，
# need_token 仍为 True，会继续生成随机 token。必须设置 PasswordIdentityProvider.hashed_password。
if _pw_hash:
    c.PasswordIdentityProvider.hashed_password = _pw_hash
elif _pw_plain:
    c.PasswordIdentityProvider.hashed_password = passwd(_pw_plain)

if _pw_hash or _pw_plain:
    c.PasswordIdentityProvider.token = ""
    c.IdentityProvider.token = ""
