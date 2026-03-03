# 阿里云部署（仅用镜像，不上传源码）

## 思路

- **本地/CI**：构建镜像 → 推送到镜像仓库（如阿里云 ACR）。
- **阿里云 ECS**：只保留 `docker-compose.image.yml` 和 `.env.aliyun`，`pull` 后 `up`，**无需任何源代码**。

---

## 一、本地：构建并推送镜像

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

```bash
# 仓库地址（结尾带 /）；版本号与代码库 tag 一致
export DOCKER_REGISTRY=crpi-v04h3vax0c07n7c5.cn-shenzhen.personal.cr.aliyuncs.com/steve-namespace/
export IMAGE_TAG=v0.1.0-beta.2

# 构建（会打上上述 registry + qdhub-backend/qdhub-frontend + IMAGE_TAG）
docker compose -f docker-compose.yml build

# 推送
docker compose -f docker-compose.yml push
```

若使用其他 ACR 或 Docker Hub，只需改 `DOCKER_REGISTRY`，例如：

```bash
export DOCKER_REGISTRY=registry.cn-hangzhou.aliyuncs.com/你的命名空间/
export IMAGE_TAG=v0.1.0-beta.2
docker compose -f docker-compose.yml build
docker compose -f docker-compose.yml push
```

---

## 二、阿里云 ECS：仅用镜像运行

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
IMAGE_TAG=v0.1.0-beta.2

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
docker login crpi-v04h3vax0c07n7c5.cn-shenzhen.personal.cr.aliyuncs.com -u linxemr --password-stdin

# 任选其一：docker-compose（独立安装）或 docker compose（插件）
docker-compose -f docker-compose.image.yml pull
docker-compose -f docker-compose.image.yml up -d
```

### 4. 访问

- **未配 ECS Nginx 时**：前端 `http://<公网IP>:3001`，后端 API `http://<公网IP>:8080`。
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

## 三、后续更新

1. **本地**：改代码后重新 `build` + `push`（同上）。
2. **阿里云**：在 `~/qdhub-deploy` 执行（确保已有 `.env`，或先 `cp .env.aliyun .env`）：

```bash
docker-compose -f docker-compose.image.yml pull
docker-compose -f docker-compose.image.yml up -d
```

无需拉取或上传任何源代码。