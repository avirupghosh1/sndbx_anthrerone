#!/usr/bin/env python3

import os
import shlex
import subprocess
import sys
from pathlib import Path

CLIENTS_DIR = Path(__file__).resolve().parent.parent / "clients"
sys.path.insert(0, str(CLIENTS_DIR / "toolbox-api-client-python-async"))
sys.path.insert(0, str(CLIENTS_DIR / "api-client-python-async"))
sys.path.insert(0, str(CLIENTS_DIR / "toolbox-api-client-python"))
sys.path.insert(0, str(CLIENTS_DIR / "api-client-python"))
sys.path.insert(0, str(CLIENTS_DIR / "sdk-python" / "src"))

from daytona import CreateSandboxFromSnapshotParams, Daytona, DaytonaConfig  # noqa: E402


API_URL = os.environ.get("API_URL") or os.environ.get("DAYTONA_API_URL")
API_KEY = os.environ.get("API_KEY") or os.environ.get("DAYTONA_API_KEY")
SNAPSHOT = os.environ.get("DAYTONA_SSH_SMOKE_SNAPSHOT", "daytona-smoke-image-snapshot")
SANDBOX_ID = os.environ.get("DAYTONA_SSH_SMOKE_SANDBOX_ID", "")

if not API_URL:
    raise RuntimeError("Set API_URL or DAYTONA_API_URL")
if not API_KEY:
    raise RuntimeError("Set API_KEY or DAYTONA_API_KEY")

os.environ["DAYTONA_API_URL"] = API_URL
os.environ["DAYTONA_API_KEY"] = API_KEY


def main():
    daytona = Daytona(DaytonaConfig(api_url=API_URL, api_key=API_KEY, target="local"))
    sandbox = None
    created = False

    try:
        if SANDBOX_ID:
            sandbox = daytona.get(SANDBOX_ID)
        else:
            sandbox = daytona.create(CreateSandboxFromSnapshotParams(snapshot=SNAPSHOT), timeout=0)
            created = True
        print(f"sandbox: {sandbox.id} state={sandbox.state}")

        ssh_access = sandbox.create_ssh_access(expires_in_minutes=10)
        print(ssh_access.ssh_command)
        validation = sandbox.validate_ssh_access(ssh_access.token)
        if not validation.valid or validation.sandbox_id != sandbox.id:
            raise AssertionError("ssh token did not validate")

        command = shlex.split(ssh_access.ssh_command) + [
            "printf ssh-ok && pwd && test -f /home/daytona/template-marker.txt"
        ]
        result = subprocess.run(command, text=True, capture_output=True, timeout=30)
        print(result.stdout.strip())
        if result.returncode != 0:
            print(result.stderr.strip())
            raise SystemExit(result.returncode)
        if "ssh-ok" not in result.stdout:
            raise AssertionError("ssh output missing")

        sandbox.revoke_ssh_access(ssh_access.token)
        validation = sandbox.validate_ssh_access(ssh_access.token)
        if validation.valid:
            raise AssertionError("ssh token still validates after revoke")
        print("daytona ssh smoke: ok")

    finally:
        if sandbox is not None and created:
            print("cleanup sandbox")
            sandbox.delete(timeout=0)


if __name__ == "__main__":
    main()
