# Jupyter Lab 研究环境：基于 scipy-notebook，预装 uv
# 用于 doc/research_and_explore 下的课题（题材分析、情绪分析、龙头模型等）
FROM jupyter/scipy-notebook:latest

USER root

# 从官方镜像拷贝 uv 二进制，避免在镜像内安装 Rust/curl
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
RUN chmod +x /usr/local/bin/uv

# 密码：before-notebook.d 注入 NOTEBOOK_ARGS（CLI 优先生效），见该脚本注释
COPY before-notebook.d/99-qdhub-auth-from-env.sh /usr/local/bin/before-notebook.d/99-qdhub-auth-from-env.sh

USER ${NB_UID}

# 容器内可访问 /mnt/data、/mnt/vdb/qdhub（由 compose 挂载）
# 如需预装常用研究依赖，可在此追加，例如：
# RUN uv pip install --system xxx
