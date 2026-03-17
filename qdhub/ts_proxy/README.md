# ts_proxy — Tushare 实时行情转发服务

内地独立部署的 Go 应用：订阅 Tushare WebSocket，将 tick 归一化后经公网 WS 流式推送给香港 QDHub，采用方案 B（RSA 交换 AES + AES 加密）。

## 构建

```bash
cd qdhub
go build -o ts_proxy ./ts_proxy
```

Linux/amd64（如内地 ECS 部署）：

```bash
make build-ts-proxy-linux-amd64   # 输出 bin/ts_proxy-linux-amd64
```

## 运行

- `TUSHARE_TOKEN` 或 `-token`：必填，Tushare 令牌。
- `RSA_PRIVATE_KEY_PATH` 或 `-rsa-key`：必填，服务端 RSA 私钥路径（用于解密客户端发来的 AES 密钥）。
- `LISTEN_ADDR` 或 `-listen`：默认 `:8888`，WS 监听地址。
- `TUSHARE_TOPIC` / `TUSHARE_CODES`：可选，默认 `HQ_STK_TICK` 与全市场代码。
- `TUSHARE_RECONNECT_MAX` 或 `-reconnect-max`：默认 30，与 Tushare 断开后最大重连次数；0 表示无上限。

```bash
./ts_proxy -token YOUR_TOKEN -rsa-key /path/to/private.pem -listen :8888
```

客户端连接 `ws://<本机IP>:8888/realtime`，首帧发送 RSA（服务端公钥）加密的 AES 密钥，之后接收 Binary 帧（AES 加密的 JSON tick）。
