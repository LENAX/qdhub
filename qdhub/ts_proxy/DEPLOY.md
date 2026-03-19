# [ts_proxy 转发服务端部署指南]()

[ts_proxy 部署在内地服务器，订阅 Tushare 实时行情 WebSocket，经方案 B（RSA 交换 AES + AES-GCM 加密）转发给香港/海外 QDHub 客户端。]()

## [一、前置条件]()

- [**Tushare Token**：在 Tushare 注册并获取接口 token，需具备实时行情权限。]()
- [**RSA 密钥对**：用于方案 B 密钥交换。服务端保留私钥，公钥提供给 QDHub 侧。]()
- [**Linux 服务器**：建议使用 linux/amd64（已提供预构建二进制）。]()

### [生成 RSA 密钥对（方案 B）]()

```bash
# 私钥（仅放在内地 ts_proxy 服务器，勿泄露）
openssl genrsa -out private.pem 2048

# 公钥（可提供给香港 QDHub 侧配置 tushare_proxy_RSA_PUBLIC_KEY_PATH）
openssl rsa -in private.pem -pubout -out public.pem
```

## [二、构建 linux/amd64 二进制]()

[在项目]() `qdhub` [目录下执行：]()

```bash
# 转发服务端
make build-ts-proxy-linux-amd64
# 输出: bin/ts_proxy-linux-amd64

# 诊断工具（用于连通性与方案 B 验证）
make build-ts-proxy-diagnose-linux-amd64
# 输出: bin/ts_proxy_diagnose-linux-amd64
```

[将生成的二进制上传到内地服务器，例如放到]() `/opt/ts_proxy/`[。]()

## [三、部署目录与权限]()

```bash
# 内地服务器示例
sudo mkdir -p /opt/ts_proxy
sudo cp bin/ts_proxy-linux-amd64 /opt/ts_proxy/ts_proxy
sudo chmod +x /opt/ts_proxy/ts_proxy

# 放置 RSA 私钥（权限收紧）
sudo cp private.pem /opt/ts_proxy/
sudo chmod 600 /opt/ts_proxy/private.pem

# 可选：创建专用用户
sudo useradd -r -s /bin/false tsproxy
sudo chown -R tsproxy:tsproxy /opt/ts_proxy
```

## [四、systemd 服务配置]()

### [4.1 安装 unit 文件]()

```bash
sudo cp qdhub/ts_proxy/deploy/ts_proxy.service /etc/systemd/system/
sudo systemctl daemon-reload
```

### [4.2 环境变量配置]()

[创建]() `/etc/ts_proxy/env`[（ts_proxy 通过环境变量读取配置）：]()

```bash
sudo mkdir -p /etc/ts_proxy
sudo tee /etc/ts_proxy/env << 'EOF'
TUSHARE_TOKEN=你的Tushare_Token
RSA_PRIVATE_KEY_PATH=/opt/ts_proxy/private.pem
LISTEN_ADDR=:8888
TUSHARE_TOPIC=HQ_STK_TICK
TUSHARE_CODES=0*.SZ,3*.SZ,6*.SH
EOF
sudo /etc/systemd/system/
```

[修改]() `ts_proxy.service` [中的]() `User=`[/]()`Group=` [若使用]() `tsproxy` [用户，确保该用户对]() `/opt/ts_proxy` [和]() `/etc/ts_proxy/env` [有读权限。]()

### [4.3 启动与开机自启]()

```bash
sudo systemctl enable ts_proxy
sudo systemctl start ts_proxy
sudo systemctl status ts_proxy
```

[查看日志：]()`journalctl -u ts_proxy -f`

## [五、诊断工具使用（ts_proxy_diagnose）]()

[将]() `ts_proxy_diagnose-linux-amd64` [放到可执行路径或香港侧机器，用于验证内地转发服务是否可达、方案 B 是否正常。]()

```bash
# 仅测试 WebSocket 连通性
./ts_proxy_diagnose-linux-amd64 -addr ws://内地服务器IP:8888/realtime

# 完整测试：连通性 + 方案 B 密钥交换 + 接收首帧并解密
./ts_proxy_diagnose-linux-amd64 -addr ws://内地服务器IP:8888/realtime -rsa-pub /path/to/public.pem
```

[输出示例：]()`CONNECT_OK`[、]()`SCHEME_B_KEY_SENT`[、]()`RECV_OK`[、]()`DIAGNOSE_OK` [表示正常。]()

## [六、QDHub 侧配置（香港/海外）]()

[QDHub 需配置为从转发端拉流（默认已为 forward）：]()

- `TUSHARE_REALTIME_SOURCE=forward`
- `tushare_proxy_WS_URL=ws://内地服务器IP:8888/realtime`
- `tushare_proxy_RSA_PUBLIC_KEY_PATH=/path/to/public.pem`[（内地提供的公钥）]()

[若使用直连 Tushare WS，则设置]() `TUSHARE_REALTIME_SOURCE=direct` [并无需配置上述 URL 与公钥。]()

## [七、防火墙与网络]()

- [内地服务器需开放 **8888** 端口（或]() `LISTEN_ADDR` [所设端口）供香港 QDHub 访问。]()
- [若内地与香港间有 VPN/专线，请确保 QDHub 所在网络能访问内地 ts_proxy 的]() `LISTEN_ADDR`[。]()

## [八、故障排查]()


| [现象]()        | [建议]()                                                   |
| ------------- | -------------------------------------------------------- |
| [连接被拒绝]()     | [检查防火墙、LISTEN_ADDR、ts_proxy 是否在运行]()                     |
| [方案 B 解密失败]() | [确认 QDHub 使用的公钥与内地私钥为一对]()                               |
| [无 tick 数据]() | [检查 Tushare Token 与权限、TUSHARE_TOPIC/TUSHARE_CODES]()     |
| [断线重连]()      | [ts_proxy 与 QDHub ForwardTickCollector 均支持自动重连，查看双方日志]() |


