#!/usr/bin/env python3
"""
QDHub 命令行客户端（Python 3.10+，建议 3.15）。
通过环境变量获取认证信息，支持 login / get / post，可将 token 写入环境变量或文件供后续调用使用。

环境变量：
  QDHUB_BASE_URL      可选，默认 https://qdhub.quantrade.team
  QDHUB_USERNAME      登录用户名（login 时必填）
  QDHUB_PASSWORD      登录密码（login 时必填）
  QDHUB_ACCESS_TOKEN   若已设置，API 调用直接使用，无需先 login
  QDHUB_TOKEN_FILE    可选，token 持久化路径；login 时写入，get/post 时若未设置 QDHUB_ACCESS_TOKEN 则从此文件读取

用法：
  python qdhub_cli.py login
      使用 QDHUB_USERNAME / QDHUB_PASSWORD 登录；
      若设置了 QDHUB_TOKEN_FILE 会写入该文件；
      标准输出打印：export QDHUB_ACCESS_TOKEN=...  便于 eval $(python qdhub_cli.py login)

  python qdhub_cli.py get /api/v1/analysis/trade-cal start_date=20250101 end_date=20251231
      对 base_url + path 发起 GET，query 参数由后续 key=value 提供；输出 JSON

  python qdhub_cli.py post /api/v1/analysis/custom-query/query --body '{"sql":"SELECT 1","max_rows":10}'
      对 base_url + path 发起 POST，--body 为 JSON 字符串；输出 JSON
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def env(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


def get_base_url() -> str:
    u = env("QDHUB_BASE_URL")
    return u or "https://qdhub.quantrade.team"


def get_token_from_file(path: str) -> str | None:
    p = Path(path).expanduser()
    if not p.is_file():
        return None
    try:
        return p.read_text().strip()
    except OSError:
        return None


def write_token_to_file(path: str, token: str) -> None:
    p = Path(path).expanduser()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(token)


def login(base_url: str, username: str, password: str) -> dict:
    import httpx

    url = f"{base_url.rstrip('/')}/api/v1/auth/login"
    payload = {"username": username, "password": password}
    r = httpx.post(url, json=payload, timeout=30.0)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict) and "access_token" in data:
        return data
    raise RuntimeError(f"Unexpected login response: {data}")


def ensure_token() -> str:
    """从环境变量或 token 文件获取 token；若没有则尝试登录并写回。"""
    token = env("QDHUB_ACCESS_TOKEN")
    if token:
        return token
    token_file = env("QDHUB_TOKEN_FILE")
    if token_file:
        token = get_token_from_file(token_file)
        if token:
            return token
    username = env("QDHUB_USERNAME")
    password = env("QDHUB_PASSWORD")
    if not username or not password:
        print("QDHUB_USERNAME and QDHUB_PASSWORD must be set for login.", file=sys.stderr)
        sys.exit(1)
    base_url = get_base_url()
    data = login(base_url, username, password)
    token = data.get("access_token") or ""
    if not token:
        print("Login did not return access_token.", file=sys.stderr)
        sys.exit(1)
    if token_file:
        write_token_to_file(token_file, token)
    return token


def cmd_login() -> None:
    base_url = get_base_url()
    username = env("QDHUB_USERNAME")
    password = env("QDHUB_PASSWORD")
    if not username or not password:
        print("Set QDHUB_USERNAME and QDHUB_PASSWORD to login.", file=sys.stderr)
        sys.exit(1)
    data = login(base_url, username, password)
    token = data.get("access_token") or ""
    if not token:
        print("Login did not return access_token.", file=sys.stderr)
        sys.exit(1)
    token_file = env("QDHUB_TOKEN_FILE")
    if token_file:
        write_token_to_file(token_file, token)
    # 输出便于 eval 的格式，便于更新当前 shell 的环境变量
    print(f"export QDHUB_ACCESS_TOKEN={json.dumps(token)}")


def _clear_token_for_retry() -> None:
    """401 后清除本地 token 以便 ensure_token() 重新登录。"""
    os.environ.pop("QDHUB_ACCESS_TOKEN", None)
    token_file = env("QDHUB_TOKEN_FILE")
    if token_file:
        try:
            Path(token_file).expanduser().unlink(missing_ok=True)
        except OSError:
            pass


def cmd_get(args: list[str]) -> None:
    import httpx

    if not args:
        print("Usage: qdhub_cli.py get <path> [key=value ...]", file=sys.stderr)
        sys.exit(1)
    path = args[0]
    params = {}
    for kv in args[1:]:
        if "=" in kv:
            k, v = kv.split("=", 1)
            params[k.strip()] = v.strip()
    base_url = get_base_url().rstrip("/")
    url = base_url + path if path.startswith("/") else base_url + "/" + path
    token = ensure_token()
    r = httpx.get(url, params=params or None, headers={"Authorization": f"Bearer {token}"}, timeout=60.0)
    if r.status_code == 401 and env("QDHUB_USERNAME") and env("QDHUB_PASSWORD"):
        _clear_token_for_retry()
        token = ensure_token()
        r = httpx.get(url, params=params or None, headers={"Authorization": f"Bearer {token}"}, timeout=60.0)
    r.raise_for_status()
    try:
        out = r.json()
    except Exception:
        out = {"_raw": r.text}
    print(json.dumps(out, ensure_ascii=False, indent=2))


def cmd_post(args: list[str]) -> None:
    import httpx

    body_str = None
    rest = []
    i = 0
    while i < len(args):
        if args[i] == "--body" and i + 1 < len(args):
            body_str = args[i + 1]
            i += 2
            continue
        rest.append(args[i])
        i += 1
    if not rest:
        print("Usage: qdhub_cli.py post <path> [--body 'json']", file=sys.stderr)
        sys.exit(1)
    path = rest[0]
    base_url = get_base_url().rstrip("/")
    url = base_url + path if path.startswith("/") else base_url + "/" + path
    token = ensure_token()
    json_body = None
    if body_str:
        try:
            json_body = json.loads(body_str)
        except json.JSONDecodeError as e:
            print(f"Invalid --body JSON: {e}", file=sys.stderr)
            sys.exit(1)
    r = httpx.post(
        url,
        json=json_body,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        timeout=60.0,
    )
    if r.status_code == 401 and env("QDHUB_USERNAME") and env("QDHUB_PASSWORD"):
        _clear_token_for_retry()
        token = ensure_token()
        r = httpx.post(
            url,
            json=json_body,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            timeout=60.0,
        )
    r.raise_for_status()
    try:
        out = r.json()
    except Exception:
        out = {"_raw": r.text}
    print(json.dumps(out, ensure_ascii=False, indent=2))


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        sys.exit(1)
    sub = sys.argv[1].lower()
    rest = sys.argv[2:]
    if sub == "login":
        cmd_login()
    elif sub == "get":
        cmd_get(rest)
    elif sub == "post":
        cmd_post(rest)
    else:
        print(f"Unknown subcommand: {sub}. Use login | get | post", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
