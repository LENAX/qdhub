# 由 docker-stacks run-hooks **source**。在 jupyter lab 启动前注入 NOTEBOOK_ARGS，
# 使 --PasswordIdentityProvider.hashed_password 以 CLI 传入（优先级高于 jupyter_server_config.py）。

if [[ -z "${JUPYTER_PASSWORD_HASH:-}" && -z "${JUPYTER_PASSWORD:-}" ]]; then
    return 0
fi

_QDHP_NB_ARGS="$(JUPYTER_PASSWORD_HASH="${JUPYTER_PASSWORD_HASH:-}" JUPYTER_PASSWORD="${JUPYTER_PASSWORD:-}" python3 <<'PY'
import os
import shlex
import sys

h = (os.environ.get("JUPYTER_PASSWORD_HASH") or "").strip()
if not h:
    from jupyter_server.auth.security import passwd

    plain = os.environ.get("JUPYTER_PASSWORD") or ""
    if not plain.strip():
        sys.exit(0)
    h = passwd(plain)
if not h:
    sys.exit(0)
sys.stdout.write(
    shlex.join(
        [
            "--PasswordIdentityProvider.hashed_password=" + h,
            "--IdentityProvider.token=",
        ]
    )
)
PY
)"

if [[ -z "${_QDHP_NB_ARGS}" ]]; then
    unset _QDHP_NB_ARGS
    return 0
fi

if [[ -n "${NOTEBOOK_ARGS:-}" ]]; then
    export NOTEBOOK_ARGS="${NOTEBOOK_ARGS} ${_QDHP_NB_ARGS}"
else
    export NOTEBOOK_ARGS="${_QDHP_NB_ARGS}"
fi
unset _QDHP_NB_ARGS
