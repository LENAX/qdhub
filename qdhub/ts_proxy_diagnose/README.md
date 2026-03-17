# ts_proxy_diagnose — 转发服务连通性诊断工具

独立应用，用于测试与 ts_proxy 转发服务的连通性及可选方案 B 端到端。

## 构建

```bash
cd qdhub
go build -o ts_proxy_diagnose ./ts_proxy_diagnose
```

Linux/amd64：

```bash
make build-ts-proxy-diagnose-linux-amd64   # 输出 bin/ts_proxy_diagnose-linux-amd64
```

## 用法

```bash
# 仅测试连通性（不进行密钥交换）
./ts_proxy_diagnose -addr ws://<内地IP>:8888/realtime

# 完整测试：连通性 + 方案 B 密钥交换 + 接收首帧并解密
./ts_proxy_diagnose -addr ws://<内地IP>:8888/realtime -rsa-pub /path/to/server_public.pem
```

- `-addr`：必填，转发服务 WebSocket 地址。
- `-rsa-pub` 或 `RSA_PUBLIC_KEY_PATH`：可选，服务端 RSA 公钥路径；若提供则执行方案 B 并验证收包解密。

输出示例：`CONNECT_OK` / `CONNECT_FAIL`、可选 `SCHEME_B_KEY_SENT`、`RECV_OK`、`DIAGNOSE_OK`。
