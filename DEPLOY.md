# 阿里云部署（仅用镜像，不上传源码）

## 思路

- **本地/CI**：构建镜像 → 推送到镜像仓库（如阿里云 ACR）。
- **阿里云 ECS**：只保留 `docker-compose.image.yml` 和 `.env.aliyun`，`pull` 后 `up`，**无需任何源代码**。

**命令区分**：开发机若安装的是独立命令 `**docker-compose`**（带连字符），文档中「二、本地」用 `docker-compose`；服务器若为 Docker 插件 `**docker compose`**（空格），文档中「三、ECS」及故障排查在 ECS 上的步骤用 `docker compose`。按你当前环境替换即可。

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

CI 仅负责构建推送，ECS 上默认使用 `latest`（`IMAGE_TAG` 可不设）；若需固定版本，再手动设置 `.env` 中 `IMAGE_TAG` 后执行：

```bash
cd ~/qdhub-deploy
# 默认可不设 IMAGE_TAG（即 latest）；需要固定版本时再设置 IMAGE_TAG（如 v0.1.0-beta.5）
docker compose -f docker-compose.image.yml pull
docker compose -f docker-compose.image.yml up -d
```

---

## 二、本地：构建并推送镜像

### 1. 镜像版本号规范

- 默认使用 `latest`（`IMAGE_TAG` 不设）。
- 生产环境建议固定到与代码库 **Git tag** 一致的版本（如 `v0.1.0-beta.2`），通过环境变量 `IMAGE_TAG` 传入。
- 示例镜像名：`<DOCKER_REGISTRY>qdhub-backend:latest`、`<DOCKER_REGISTRY>qdhub-frontend:latest`（或固定版本 tag）。

### 2. 登录镜像仓库（阿里云个人实例示例）

```bash
# 阿里云容器镜像服务 - 个人实例（深圳）
docker login crpi-v04h3vax0c07n7c5.cn-shenzhen.personal.cr.aliyuncs.com -u linxemr
# 输入 ACR 用户名、密码
```

### 3. 构建并推送（带版本号）

在项目根目录（有 `docker-compose.yml` 的目录）：

- **若在 Mac ARM（M1/M2）上构建、且要部署到阿里云 ECS（x86/amd64）**：必须先按**目标平台 amd64** 构建，否则 ECS 会报 `image's platform (linux/arm64) does not match the detected host platform (linux/amd64)`。见下方“仅 ECS 用”或“多架构”两种方式。
- **为何 Docker 里 `go build` 比本机慢很多**：在 Mac ARM 上设 `DOCKER_DEFAULT_PLATFORM=linux/amd64` 时，构建在 **QEMU 模拟的 x86** 里跑；后端 **CGO（DuckDB）** 会调用 gcc，模拟下往往比本机原生慢一个数量级，属正常现象。**缓策**：发版优先用 **CI（linux/amd64）** 构建；本地重复构建依赖 Dockerfile 里的 BuildKit **Go / npm 缓存挂载**（`GOMODCACHE`、`GOCACHE`、`npm` 缓存），改代码后的增量构建会明显快于首次。

**仅 ECS 用（单架构 amd64，推荐在 Mac 上为 ECS 构建时用）：**

```bash
export DOCKER_REGISTRY=crpi-v04h3vax0c07n7c5.cn-shenzhen.personal.cr.aliyuncs.com/steve-namespace/
export IMAGE_TAG=v0.1.0-beta.3

# 指定目标平台为 linux/amd64（ECS 常见架构）；开发机为 docker-compose 时用 docker-compose
DOCKER_DEFAULT_PLATFORM=linux/amd64 docker compose -f docker-compose.yml build
docker compose -f docker-compose.yml push
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
- **.env.aliyun**：环境变量。`IMAGE_TAG` 可留空以使用默认 `latest`，也可设置为固定版本（见下方示例）。部署时复制为 **.env**，便于兼容不支持 `--env-file` 的旧版 docker-compose。
- `**pull` 报 `manifest unknown`（qdhub-backend / qdhub-frontend）**：说明 `**.env` 里 `IMAGE_TAG` 写成了仅存在于 `qdhub-jupyter-lab` 的 tag**（如 `v0.1.0-jupyter.1`）。请改为 ACR 里**确有**的 backend/frontend tag（例如 `v0.1.1-beta.5`），并把 Jupyter 专用 tag 只写在 `**JUPYTER_IMAGE_TAG`**（见「五、Jupyter」）。改完后执行：`docker compose -f docker-compose.image.yml config | grep image:` 确认解析出的镜像名正确。

```bash
# 镜像仓库（与推送时一致，结尾带 /）及版本号（可选）
DOCKER_REGISTRY=crpi-v04h3vax0c07n7c5.cn-shenzhen.personal.cr.aliyuncs.com/steve-namespace/
# IMAGE_TAG 不设时默认 latest；如需固定版本可取消注释
# IMAGE_TAG=v0.1.1-beta.5

# 数据与日志（阿里云数据盘）
QDHUB_DATA_DIR=/mnt/vdb/qdhub/data
QDHUB_LOG_DIR=/mnt/vdb/qdhub/logs

# ts_proxy 公钥目录（其下放置 public.pem）；挂载为容器内 /root/.key，与默认 rsa_public_key_path 一致
QDHUB_KEY_DIR=/mnt/vdb/qdhub/keys

# 实时行情 ts_realtime_mkt_tick：经内地 ts_proxy（forward），勿留空否则交易时段计划启动会失败（不会静默改连 Tushare 直连）
TUSHARE_REALTIME_SOURCE=forward
TUSHARE_PROXY_WS_URL=ws://47.107.235.54:8888/realtime
TUSHARE_PROXY_RSA_PUBLIC_KEY_PATH=/root/.key/public.pem

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

### 2. 创建数据目录与密钥目录

```bash
sudo mkdir -p /mnt/vdb/qdhub/data /mnt/vdb/qdhub/logs /mnt/vdb/qdhub/keys
sudo chown -R $USER:$USER /mnt/vdb/qdhub
```

**ts_proxy（Tushare 转发）公钥**：将内地提供的 `public.pem` 放到 `**QDHUB_KEY_DIR`（默认 `/mnt/vdb/qdhub/keys`）** 下，命名为 `**public.pem`**。`docker-compose.image.yml` 已将该目录**只读**挂载到容器内 `**/root/.key`**，与库里默认的 `rsa_public_key_path` 一致。若公钥放在其他路径，可改 `.env` 中的 `QDHUB_KEY_DIR`，或改数据库 `realtime_sources` 里对应源的 `rsa_public_key_path` 与挂载目标一致。

**ts_proxy WebSocket 地址**：`.env` 中设置 `**TUSHARE_PROXY_WS_URL`**（示例见上文，内地转发机默认路径为 `/realtime`、端口 `8888`），并保持 `**TUSHARE_REALTIME_SOURCE=forward**`。后端会将该地址与库表 `realtime_sources` 中 `tushare_proxy` 的配置合并（与启动健康检查规则一致）。香港 ECS 需能访问该 `ws://` 主机与端口（内地安全组放行香港出口 IP）。连通性可用仓库内 `qdhub/ts_proxy_diagnose` 自测。

**注意**：`public.pem` 权限建议仅运维用户可读（如 `chmod 600`），勿提交到 Git。

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
- **配好 ECS Nginx（推荐）**：前端 `https://你的域名`（或 `http://` 跳转），由宿主机 Nginx 监听 80/443，反代到 `127.0.0.1:3001`。生产可参考仓库 `**deploy/qdhub.conf`**（含 SSE、`/api/v1/ws/`、Let’s Encrypt 路径等）；下方「5. ECS 上 Nginx」为原理示例，以仓库 `deploy/` 为准合并。

### 5. ECS 上 Nginx 做 HTTPS（可选）

前端容器只暴露 **3001**，后端 API 为 **8080**，80/443 由 ECS 宿主机 Nginx 监听，便于挂证书、做 HTTPS。

**要点**：`/api/v1/ws/`*（如实时行情 WebSocket）必须在 Nginx 上按 **WebSocket 升级**转发：需要 `Upgrade` 与 `Connection: upgrade`，且 **不能**像普通 HTTP 反代那样把 `Connection` 置空。否则上游 Go（`gorilla/websocket`）收不到合法握手，会返回 **400 Bad Request**（响应体约 12 字节），Postman 里表现为 `Unexpected server response: 400`。

```nginx
# /etc/nginx/sites-available/quantrade.team 示例（含 API + WS）
server {
    listen 443 ssl;
    server_name quantrade.team www.quantrade.team;

    ssl_certificate     /etc/letsencrypt/live/quantrade.team/fullchain.pem;
    ssl_certificate_key  /etc/letsencrypt/live/quantrade.team/privkey.pem;

    # WebSocket：必须写在 location /api/ 之前（更长前缀优先匹配）
    location /api/v1/ws/ {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 3600s;
    }

    # 普通 REST API
    location /api/ {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

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

**替代写法（单块 `/api/`）**：若不想拆两个 `location`，可在 `http` 块用 `map` 根据 `$http_upgrade` 设置 `Connection`，再对 `/api/` 统一 `proxy_set_header Connection $connection_upgrade`（见 [Nginx WebSocket 代理文档](http://nginx.org/en/docs/http/websocket.html)）。拆 `location /api/v1/ws/` 通常更直观、不易配错。

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

说明：`docker-compose.yml` 与 `docker-compose.image.yml` 已配置 `pull_policy: always`，执行 `up` 时会优先尝试拉取远端最新镜像。

无需拉取或上传任何源代码。

---

## 五、Jupyter Lab 研究环境镜像（可选）

若需要在服务器上使用 Jupyter Lab，**须先在本地/CI 构建并推送 `qdhub-jupyter-lab` 镜像**，ECS 只 `pull` + `up`，不在服务器上 `build`。

**与主站 tag 分离**：`docker-compose.image.yml` 使用 `**IMAGE_TAG`**（对应 `qdhub-backend` / `qdhub-frontend`）。`docker-compose.jupyter.yml` 使用 `**JUPYTER_IMAGE_TAG**`（仅 `qdhub-jupyter-lab`）。同一 `.env` 可同时写二者，**切勿**把 Jupyter 专用 tag（如 `v0.1.0-jupyter.1`）赋给 `IMAGE_TAG`，否则主站 `pull` 会报 `manifest unknown`。

访问方式二选一（**同一容器实例不要混用**两种 URL 形态，见 `JUPYTER_BASE_URL`）：


| 方式   | Nginx                                                                                        | 环境变量                                            |
| ---- | -------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| 路径前缀 | 主站配置里 `location /jupyter/`（见 `**deploy/qdhub.conf`** 若保留该段）                                  | `JUPYTER_BASE_URL` 默认 `/jupyter/`（compose 默认即可） |
| 独立子域 | 单独 `**deploy/jupyter.conf**`，`server_name` 如 `jupyter.quantrade.team`，Let’s Encrypt 单独签发该主机名 | `export JUPYTER_BASE_URL=/`                     |


容器内挂载 `**deploy/jupyter_server_config.py**`（compose 已配置）；**密码**用 `JUPYTER_PASSWORD` 或 `JUPYTER_PASSWORD_HASH`（与 **非空** `JUPYTER_TOKEN` 互斥）。修改密码或 `JUPYTER_BASE_URL` 后须 `**docker compose -f docker-compose.jupyter.yml up -d --force-recreate`**。

### 1. 本地构建并推送（linux/amd64）

在项目根目录：

```bash
export DOCKER_REGISTRY=crpi-v04h3vax0c07n7c5.cn-shenzhen.personal.cr.aliyuncs.com/steve-namespace/
export JUPYTER_IMAGE_TAG=v0.1.0-jupyter.1

# 使用 buildx 按 linux/amd64 构建并推送（与 research-env/README.md 一致）
docker buildx create --name qdhub-jupyter --use 2>/dev/null || true
docker buildx inspect --bootstrap

docker buildx build \
  --platform linux/amd64 \
  -t "${DOCKER_REGISTRY}qdhub-jupyter-lab:${JUPYTER_IMAGE_TAG}" \
  -f research-env/Dockerfile \
  research-env \
  --push
```

### 2. ECS 上使用 Jupyter 镜像

在 ECS 的**含 `docker-compose.jupyter.yml` 与 `deploy/` 的目录**（例如从仓库同步 `deploy/` 与 compose 文件）执行：

```bash
export DOCKER_REGISTRY=crpi-v04h3vax0c07n7c5.cn-shenzhen.personal.cr.aliyuncs.com/steve-namespace/
export JUPYTER_IMAGE_TAG=v0.1.0-jupyter.1   # 必须与构建时一致；勿与主站 IMAGE_TAG 混用

export JUPYTER_MOUNT_DATA=/mnt/data
export JUPYTER_MOUNT_QDHUB=/mnt/vdb/qdhub
export JUPYTER_PORT=8888

# 子域名 HTTPS 反代时必设根路径：
export JUPYTER_BASE_URL=/

# 推荐：密码登录（勿再设置非空的 JUPYTER_TOKEN）
export JUPYTER_PASSWORD='你的强密码'

# 若不用密码、仅用 token，可：export JUPYTER_TOKEN=your-secret（且不要设 JUPYTER_PASSWORD）

docker compose -f docker-compose.jupyter.yml pull jupyter-lab
docker compose -f docker-compose.jupyter.yml up -d --force-recreate
```

**Nginx**：子域方式将 `**deploy/jupyter.conf`** 拷至 `/etc/nginx/conf.d/`，证书目录为 `/etc/letsencrypt/live/<jupyter 主机名>/`；首次无证书时可先只启用 80 段再 `certbot certonly --nginx -d <主机名>`（见 `jupyter.conf` 文件头注释）。仅用路径方式时保证 `**deploy/jupyter_server_config.py` 与 `JUPYTER_BASE_URL`、主站 `location /jupyter/` 一致**。

更细的构建与变量说明见 `**research-env/README.md`**。