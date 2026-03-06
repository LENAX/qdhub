"""
Jupyter Server 配置（用于 nginx 反向代理到 /jupyter/ 的场景）。

参考 docker-compose.jupyter.yml 中的 jupyter-lab 服务，通过挂载到
/home/jovyan/.jupyter/jupyter_server_config.py 生效。
"""

from jupyter_server.serverapp import ServerApp  # noqa: F401  保证在新版本下属性名正确

c = get_config()  # type: ignore[name-defined]

# 在 nginx 下通过 https://<host>/jupyter/ 访问
c.ServerApp.base_url = "/jupyter/"

# 信任 X-Forwarded-* 头，让 Jupyter 正确认知外部是 HTTPS
c.ServerApp.trust_xheaders = True
c.ServerApp.preferred_url_scheme = "https"

# 基础网络配置，与 docker-compose 中端口映射保持一致
c.ServerApp.ip = "0.0.0.0"
c.ServerApp.port = 8888
c.ServerApp.open_browser = False

