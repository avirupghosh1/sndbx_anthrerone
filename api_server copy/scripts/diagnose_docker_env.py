#!/usr/bin/env python3
"""Compare Docker connectivity: same interpreter + env as ``uvicorn`` vs ``docker`` CLI.

Run from ``api_server/`` (or set PYTHONPATH) after activating the same venv you use for the API::

  cd api_server && source ../.venv/bin/activate
  python scripts/diagnose_docker_env.py

If this script's ``docker.from_env()`` fails but ``docker info`` works in Terminal, the API
process likely uses a **different Python**, **venv**, or **environment** (missing ``DOCKER_HOST``).
"""

from __future__ import annotations

import os
import subprocess
import sys


def main() -> int:
    print("== Python")
    print(sys.executable)
    print(sys.version)
    print()

    print("== Relevant env")
    for k in ("DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH"):
        v = os.environ.get(k)
        print(f"  {k}={v!r}")
    home = os.path.expanduser("~")
    colima_sock = os.path.join(home, ".colima", "default", "docker.sock")
    if os.path.exists(colima_sock):
        print(f"  (Colima socket exists: {colima_sock} — set DOCKER_HOST=unix://{colima_sock} if not using default context)")
    print()

    print("== docker info (CLI, shell)")
    try:
        r = subprocess.run(
            ["docker", "info", "-f", "{{.ServerVersion}}"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        print("  exit:", r.returncode)
        print("  stdout:", (r.stdout or "").strip() or "(empty)")
        if r.stderr:
            print("  stderr:", r.stderr[:500])
    except FileNotFoundError:
        print("  (docker CLI not on PATH)")
    except Exception as ex:  # noqa: BLE001
        print("  error:", ex)
    print()

    print("== docker.from_env() (same Python as this script)")
    try:
        import docker

        c = docker.from_env()
        v = c.version().get("Version", "?")
        print("  OK — Engine API version:", v)
        c.ping()
        print("  ping: OK")
    except Exception as ex:  # noqa: BLE001
        print("  FAIL:", type(ex).__name__, ex)
        print()
        print("Fix: use this same ``python`` to run uvicorn, or fix DOCKER_HOST / install docker SDK deps.")
        return 1

    print()
    print("If the above OK but the API still logs 'Docker client not available', restart uvicorn")
    print("after Docker is up (the API now retries docker.from_env() on each sandbox operation).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
