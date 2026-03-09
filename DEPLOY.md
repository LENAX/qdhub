# 阿里云部署（仅用镜像，不上传源码）

## 思路

- **本地/CI**：构建镜像 → 推送到镜像仓库（如阿里云 ACR）。
- **阿里云 ECS**：只保留 `docker-compose.image.yml` 和 `.env.aliyun`，`pull` 后 `up`，**无需任何源代码**。

**命令区分**：开发机若安装的是独立命令 `**docker-compose`**（带连字符），文档中「二、本地」用 `docker-compose`；服务器若为 Docker 插件 `**docker compose**`（空格），文档中「三、ECS」及故障排查在 ECS 上的步骤用 `docker compose`。按你当前环境替换即可。

---

## 一、CI 构建与推送（推荐）

GitHub Actions 在 **main 分支 push tag**（格式 `v`*）时自动构建 `qdhub-backend`、`qdhub-frontend` 并推送到阿里云 ACR，镜像 tag 与 Git tag 一致。

### 1. 配置 GitHub Secrets

在仓库 **Settings → Secrets and variables → Actions** 中添加：


| Secret 名        | 说明                    | 示例值                                                          |
| --------------- | --------------------- | ------------------------------------------------------------ |
| `ACR_REGISTRY`  | 阿里云 ACR 注册表地址（不含命名空间） | `crpi-v04h3vax0c07n7c5.cn-shenzhen.personal.cr.aliyuncs.com` |
| `ACR_NAMESPACE` | 命名空间                  | `steve-namespace`                                            |
| `ACR_USERNAME`  | ACR 登录用户名             | `linxemr`                                                    |
| `ACR_PASSWORD`  | ACR 登录密码              | （你的密码）                                                       |


### 2. 发布流程

```bash
# 在 main 上打 tag 并推送
git tag v0.1.0-beta.5
git push origin v0.1.0-beta.5
```

CI 会自动构建并推送 `qdhub-backend:v0.1.0-beta.5`、`qdhub-frontend:v0.1.0-beta.5` 到阿里云 ACR。

### 3. ECS 部署

CI 仅负责构建推送，ECS 上需**手动**更新 `.env` 中的 `IMAGE_TAG` 后执行：

```bash
cd ~/qdhub-deploy
# 编辑 .env，将 IMAGE_TAG 改为新版本（如 v0.1.0-beta.5）
docker compose -f docker-compose.image.yml pull
docker compose -f docker-compose.image.yml up -d
```

---

## 二、本地：构建并推送镜像

### 1. 镜像版本号规范

- 与代码库 **Git tag** 保持一致，例如当前为 `v0.1.0-beta.2`。
- 通过环境变量 **IMAGE_TAG** 传入，不设则默认为 `latest`。
- 最终镜像名示例：`<DOCKER_REGISTRY>qdhub-backend:v0.1.0-beta.2`、`<DOCKER_REGISTRY>qdhub-frontend:v0.1.0-beta.2`。

### 2. 登录镜像仓库（阿里云个人实例示例）

```bash
# 阿里云容器镜像服务 - 个人实例（深圳）
docker login crpi-v04h3vax0c07n7c5.cn-shenzhen.personal.cr.aliyuncs.com -u linxemr
# 输入 ACR 用户名、密码
```

### 3. 构建并推送（带版本号）

在项目根目录（有 `docker-compose.yml` 的目录）：

- **若在 Mac ARM（M1/M2）上构建、且要部署到阿里云 ECS（x86/amd64）**：必须先按**目标平台 amd64** 构建，否则 ECS 会报 `image's platform (linux/arm64) does not match the detected host platform (linux/amd64)`。见下方“仅 ECS 用”或“多架构”两种方式。

**仅 ECS 用（单架构 amd64，推荐在 Mac 上为 ECS 构建时用）：**

```bash
export DOCKER_REGISTRY=crpi-v04h3vax0c07n7c5.cn-shenzhen.personal.cr.aliyuncs.com/steve-namespace/
export IMAGE_TAG=v0.1.0-beta.3

# 指定目标平台为 linux/amd64（ECS 常见架构）；开发机为 docker-compose 时用 docker-compose
DOCKER_DEFAULT_PLATFORM=linux/amd64 docker-compose -f docker-compose.yml build
docker-compose -f docker-compose.yml push
```

**本机与 ECS 同架构时（或本机为 x86）：**

```bash
# 仓库地址（结尾带 /）；版本号与代码库 tag 一致
export DOCKER_REGISTRY=crpi-v04h3vax0c07n7c5.cn-shenzhen.personal.cr.aliyuncs.com/steve-namespace/
export IMAGE_TAG=v0.1.0-beta.3

docker-compose -f docker-compose.yml build
docker-compose -f docker-compose.yml push
```

若使用其他 ACR 或 Docker Hub，只需改 `DOCKER_REGISTRY`，例如：

```bash
export DOCKER_REGISTRY=registry.cn-hangzhou.aliyuncs.com/你的命名空间/
export IMAGE_TAG=v0.1.0-beta.2
docker-compose -f docker-compose.yml build
docker-compose -f docker-compose.yml push
```

### 4. 多架构构建（同时支持 linux/amd64 与 linux/arm64）

在 Mac ARM64 上若希望**一次构建并推送**两个架构（ECS 常用 amd64，本机为 arm64），使用 buildx 多平台构建，同一 tag 下会包含两个架构，拉取时自动匹配宿主机架构：

```bash
export DOCKER_REGISTRY=crpi-v04h3vax0c07n7c5.cn-shenzhen.personal.cr.aliyuncs.com/steve-namespace/
export IMAGE_TAG=v0.1.0-beta.4

# 确保使用支持多平台的 builder
docker buildx create --name multiarch --use 2>/dev/null || true
docker buildx use multiarch
docker buildx inspect --bootstrap

# 构建并推送（直接 push，不 --load）
docker buildx build --platform linux/amd64,linux/arm64 \
  -t "${DOCKER_REGISTRY}qdhub-backend:${IMAGE_TAG}" --push ./qdhub
docker buildx build --platform linux/amd64,linux/arm64 \
  -t "${DOCKER_REGISTRY}qdhub-frontend:${IMAGE_TAG}" --push ./qdhub-frontend
```

- **仅构建单架构**：本机 arm64 用 `docker-compose -f docker-compose.yml build`；仅要 amd64 时用  
`DOCKER_DEFAULT_PLATFORM=linux/amd64 docker-compose -f docker-compose.yml build`。（开发机若为插件版则把 `docker-compose` 改为 `docker compose`。）

---

## 三、阿里云 ECS：仅用镜像运行

### 1. 准备目录与文件（无需源码）

在 ECS 上创建目录，只放两个文件：

```bash
mkdir -p ~/qdhub-deploy
cd ~/qdhub-deploy
```

- **docker-compose.image.yml**：从本仓库复制一份（或只保留这一份 compose）。
- **.env.aliyun**：环境变量，**IMAGE_TAG 需与构建时一致**（见下方示例）。部署时复制为 **.env**，便于兼容不支持 `--env-file` 的旧版 docker-compose。

```bash
# 镜像仓库（与推送时一致，结尾带 /）及版本号
DOCKER_REGISTRY=crpi-v04h3vax0c07n7c5.cn-shenzhen.personal.cr.aliyuncs.com/steve-namespace/
IMAGE_TAG=v0.1.0-beta.4

# 数据与日志（阿里云数据盘）
QDHUB_DATA_DIR=/mnt/vdb/qdhub/data
QDHUB_LOG_DIR=/mnt/vdb/qdhub/logs

# 生产环境安全配置（强烈建议修改）
# - QDHUB_SERVER_ENABLE_SWAGGER=false           关闭 /swagger、/docs
# - QDHUB_AUTH_ADMIN_PASSWORD=<随机强密码>     覆盖默认 admin123（admin 账号）
# - QDHUB_AUTH_GUEST_PASSWORD=<随机强密码>     覆盖默认 guest123（guest 只读账号）
#   说明：
#   - 服务启动时会先跑默认迁移，插入 admin/admin123 与 guest/guest123 账号；
#   - 随后分别读取 auth.admin_password 与 auth.guest_password，
#     并通过环境变量 QDHUB_AUTH_ADMIN_PASSWORD / QDHUB_AUTH_GUEST_PASSWORD 覆盖密码哈希。
#   - 因此只要设置了以上变量，最终 admin/guest 的密码一定是你配置的强密码，而不是默认值。
QDHUB_SERVER_ENABLE_SWAGGER=false
QDHUB_AUTH_ADMIN_PASSWORD=你的强密码
QDHUB_AUTH_GUEST_PASSWORD=你的强密码（只读）
```

### 2. 创建数据目录

```bash
sudo mkdir -p /mnt/vdb/qdhub/data /mnt/vdb/qdhub/logs
sudo chown -R $USER:$USER /mnt/vdb/qdhub
```

### 3. 登录仓库并拉取、启动

```bash
cd ~/qdhub-deploy   
cp .env.aliyun .env   # Compose 自动读当前目录 .env，兼容旧版（无 --env-file）
docker login crpi-v04h3vax0c07n7c5.cn-shenzhen.personal.cr.aliyuncs.com -u linxemr

# 服务器上为 Docker 插件时用：docker compose（空格）；若为独立安装则用 docker-compose
docker compose -f docker-compose.image.yml pull
docker compose -f docker-compose.image.yml up -d
```

### 4. 访问

- **当前服务器**：已迁移至香港，公网 IP `43.99.17.169`（原服务器已停用）。
- **未配 ECS Nginx 时**：前端 `http://43.99.17.169:3001`，后端 API `http://43.99.17.169:8080`。
- **配好 ECS Nginx（推荐）**：前端 `https://你的域名`（或 `http://` 跳转），由宿主机 Nginx 监听 80/443，反代到 `127.0.0.1:3001`。

### 5. ECS 上 Nginx 做 HTTPS（可选）

前端容器只暴露 **3001**，80/443 由 ECS 宿主机 Nginx 监听，便于挂证书、做 HTTPS：

```nginx
# /etc/nginx/sites-available/quantrade.team 示例
server {
    listen 443 ssl;
    server_name quantrade.team www.quantrade.team;

    ssl_certificate     /etc/letsencrypt/live/quantrade.team/fullchain.pem;
    ssl_certificate_key  /etc/letsencrypt/live/quantrade.team/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:3001;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}

server {
    listen 80;
    server_name quantrade.team www.quantrade.team;
    return 301 https://$host$request_uri;
}
```

启用：`sudo ln -s /etc/nginx/sites-available/quantrade.team /etc/nginx/sites-enabled/`，`sudo nginx -t && sudo systemctl reload nginx`。

---

## 故障排查：backend unhealthy

若出现 `dependency failed to start: container ... backend ... is unhealthy`：

1. **ECS 上请用 `docker-compose.image.yml`**，不要用 `docker-compose.yml`（后者含 build，且默认数据目录为 `./qdhub/data`，在部署目录下可能不存在或权限不对）：
  ```bash
   docker compose -f docker-compose.image.yml up -d
  ```
   （服务器用 `docker compose`；若为独立安装则改为 `docker-compose`。）
2. **查看 backend 日志**，确认是启动慢还是进程退出：
  ```bash
   docker compose -f docker-compose.image.yml logs backend
   # 或
   docker logs qdhub-deploy-backend-1
  ```
  - 若为 **migration 失败、permission denied、failed to initialize** 等，按报错修（目录权限、.env 中 `QDHUB_DATA_DIR`/`QDHUB_LOG_DIR` 等）。
  - 若为 **启动较慢**（首次迁移多），已把健康检查的 `start_period` 改为 30s、`retries` 改为 5，可再试一次。
3. **确认数据与日志目录存在且可写**：
  ```bash
   sudo mkdir -p /mnt/vdb/qdhub/data /mnt/vdb/qdhub/logs
   sudo chown -R $USER:$USER /mnt/vdb/qdhub
  ```
4. **确认 .env 中必填项**：`docker-compose.image.yml` 里 `QDHUB_AUTH_ADMIN_PASSWORD` 为必填（无默认值），未设置时 compose 可能报错或行为异常。

**镜像平台不匹配**（`The requested image's platform (linux/arm64) does not match the detected host platform (linux/amd64)`）：  
镜像是在 Mac ARM 上按默认架构构建的，而 ECS 是 x86/amd64。需在**开发机**重新按 **linux/amd64** 构建并推送同一 tag（开发机用 `docker-compose`）：

```bash
DOCKER_DEFAULT_PLATFORM=linux/amd64 docker-compose -f docker-compose.yml build
docker-compose -f docker-compose.yml push
```

然后在 **ECS** 上 `docker compose -f docker-compose.image.yml pull` 再 `up -d`。也可用上文「多架构构建」一次推送 amd64+arm64。

---

## 四、后续更新

1. **构建镜像**：优先用 **CI**（push tag 触发，见「一、CI 构建与推送」）；或本地 `build` + `push`（见「二、本地」）。
2. **阿里云 ECS**：在 `~/qdhub-deploy` 执行（确保已有 `.env`，或先 `cp .env.aliyun .env`），用 `**docker compose`**（插件）：

```bash
docker compose -f docker-compose.image.yml pull
docker compose -f docker-compose.image.yml up -d
```

无需拉取或上传任何源代码。

---

## 五、Jupyter Lab 研究环境镜像（可选）

若需要在服务器上通过 Nginx 以 `https://<域名>/jupyter/` 方式访问 Jupyter Lab，**必须在本地先构建并推送 Jupyter 镜像**，ECS 只从镜像仓库拉取。

### 1. 本地构建并推送（linux/amd64）

在项目根目录：

```bash
export DOCKER_REGISTRY=crpi-v04h3vax0c07n7c5.cn-shenzhen.personal.cr.aliyuncs.com/steve-namespace/
export IMAGE_TAG=v0.1.0-jupyter.1

# 使用 buildx 按 linux/amd64 构建并推送（与 research-env/README.md 一致）
docker buildx create --name qdhub-jupyter --use 2>/dev/null || true
docker buildx inspect --bootstrap

docker buildx build \
  --platform linux/amd64 \
  -t "${DOCKER_REGISTRY}qdhub-jupyter-lab:${IMAGE_TAG}" \
  -f research-env/Dockerfile \
  research-env \
  --push
```

### 2. ECS 上使用 Jupyter 镜像

在 ECS 上，按照 `research-env/README.md` 中示例设置：

```bash
export DOCKER_REGISTRY=crpi-v04h3vax0c07n7c5.cn-shenzhen.personal.cr.aliyuncs.com/steve-namespace/
export IMAGE_TAG=v0.1.0-jupyter.1   # 必须与本地构建时一致

export JUPYTER_MOUNT_DATA=/mnt/data/jupyter
export JUPYTER_MOUNT_QDHUB=/mnt/data/qdhub
export JUPYTER_PORT=8888
export JUPYTER_TOKEN=your-secret

docker compose -f docker-compose.jupyter.yml pull jupyter-lab
docker compose -f docker-compose.jupyter.yml up -d
```

ECS 只需拉取 `qdhub-jupyter-lab` 镜像并启动，不在服务器上 `build`，这样可避免访问 Docker Hub / GHCR 以及多架构问题。详细说明可参考 `research-env/README.md`。