# Jupyter Lab 研究环境

用于 [research-topics.md](../doc/research_and_explore/research-topics.md) 中的课题（题材分析、情绪分析、龙头模型等）。

- **基础镜像**：`jupyter/scipy-notebook`（含 pandas、numpy、scipy 等）
- **已装**：`uv`（Python 包管理，可在 notebook 里用 `uv pip install xxx`）

## 用法

在项目根目录：

```bash
# 构建并启动
docker compose -f docker-compose.jupyter.yml up -d --build

# 查看日志（首次会打印访问 URL 与 token）
docker compose -f docker-compose.jupyter.yml logs -f jupyter-lab
```

浏览器打开日志中的 `http://127.0.0.1:8888/lab?token=...` 即可。

## 挂载目录


| 宿主机路径            | 容器内路径            | 说明  |
| ---------------- | ---------------- | --- |
| `/mnt/data`      | `/mnt/data`      | 只读  |
| `/mnt/vdb/qdhub` | `/mnt/vdb/qdhub` | 读写  |


在服务器上若实际路径不同，可通过环境变量覆盖：

```bash
export JUPYTER_MOUNT_DATA=/your/data/path
export JUPYTER_MOUNT_QDHUB=/your/qdhub/path
export JUPYTER_PORT=8888
export JUPYTER_TOKEN=your-secret   # 可选，不设则启动时在日志里生成
docker compose -f docker-compose.jupyter.yml up -d
```

## 本机开发

本机若无 `/mnt/data`、`/mnt/vdb/qdhub`，Docker 会创建空目录并挂载。可用上面变量指向本地目录，例如：

```bash
export JUPYTER_MOUNT_DATA=./data
export JUPYTER_MOUNT_QDHUB=.
docker compose -f docker-compose.jupyter.yml up -d
```

## 在本地构建并推送到阿里云 ACR（linux/amd64）

与主站 backend/frontend 一样，Jupyter 镜像也支持通过 `DOCKER_REGISTRY` + `IMAGE_TAG` 的方式管理，并推送到阿里云 ACR，供服务器直接 `pull` 使用。

### 1. 在本地构建并推送

在项目根目录：

```bash
# 1）登录阿里云 ACR（示例：个人实例）
docker login crpi-v04h3vax0c07n7c5.cn-shenzhen.personal.cr.aliyuncs.com -u linxemr

# 2）与 DEPLOY.md 中一致的环境变量
export DOCKER_REGISTRY=crpi-v04h3vax0c07n7c5.cn-shenzhen.personal.cr.aliyuncs.com/steve-namespace/
export IMAGE_TAG=v0.1.0-jupyter.1   # 示例，可与主站 tag 对齐或单独命名

# 3）使用 buildx 按 linux/amd64 构建并推送（适配阿里云 ECS x86/amd64）
docker buildx create --name qdhub-jupyter --use 2>/dev/null || true
docker buildx inspect --bootstrap

docker buildx build \
  --platform linux/amd64 \
  -t "${DOCKER_REGISTRY}qdhub-jupyter-lab:${IMAGE_TAG}" \
  -f research-env/Dockerfile \
  research-env \
  --push
```

构建完成后，会在 ACR 中得到类似：

`crpi-v04h3vax0c07n7c5.cn-shenzhen.personal.cr.aliyuncs.com/steve-namespace/qdhub-jupyter-lab:v0.1.0-jupyter.1`

你也可以使用 `DOCKER_DEFAULT_PLATFORM=linux/amd64 docker compose -f docker-compose.jupyter.yml build` 的方式本地构建，再用 `docker push` 推送，但上面的 buildx 方式更清晰直接。

### 2. 在服务器上使用 ACR 中的镜像

在服务器上（如阿里云 ECS），只需拉取并启动，不再从公网构建基础镜像：

```bash
export DOCKER_REGISTRY=crpi-v04h3vax0c07n7c5.cn-shenzhen.personal.cr.aliyuncs.com/steve-namespace/
export IMAGE_TAG=v0.1.0-jupyter.1   # 必须与本地构建时一致

export JUPYTER_MOUNT_DATA=/mnt/data/jupyter       # 按实际路径调整
export JUPYTER_MOUNT_QDHUB=/mnt/data/qdhub  # 按实际路径调整
export JUPYTER_PORT=8888
export JUPYTER_TOKEN=your-secret           # 可选；不设则在日志中查看自动生成的 token

docker login crpi-v04h3vax0c07n7c5.cn-shenzhen.personal.cr.aliyuncs.com -u linxemr

docker compose -f docker-compose.jupyter.yml pull jupyter-lab
docker compose -f docker-compose.jupyter.yml up -d
```

注意在服务器上**不要使用 `--build`**，只需 `pull + up` 即可，这样 Jupyter 会直接使用你在本地构建并推送到 ACR 的 `qdhub-jupyter-lab` 镜像，避免服务器访问 Docker Hub / GHCR。