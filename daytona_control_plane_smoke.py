#!/usr/bin/env python3

import os
import sys
from pathlib import Path

CLIENTS_DIR = Path(__file__).resolve().parent.parent / "clients"
sys.path.insert(0, str(CLIENTS_DIR / "toolbox-api-client-python-async"))
sys.path.insert(0, str(CLIENTS_DIR / "api-client-python-async"))
sys.path.insert(0, str(CLIENTS_DIR / "toolbox-api-client-python"))
sys.path.insert(0, str(CLIENTS_DIR / "api-client-python"))
sys.path.insert(0, str(CLIENTS_DIR / "sdk-python" / "src"))

from daytona import (  # noqa: E402
    CreateSandboxFromImageParams,
    CreateSandboxFromSnapshotParams,
    CreateSnapshotParams,
    Daytona,
    DaytonaConfig,
    Image,
)


API_URL = os.environ.get("API_URL") or os.environ.get("DAYTONA_API_URL")
API_KEY = os.environ.get("API_KEY") or os.environ.get("DAYTONA_API_KEY")

if not API_URL:
    raise RuntimeError("Set API_URL or DAYTONA_API_URL")
if not API_KEY:
    raise RuntimeError("Set API_KEY or DAYTONA_API_KEY")

os.environ["DAYTONA_API_URL"] = API_URL
os.environ["DAYTONA_API_KEY"] = API_KEY


def main():
    daytona = Daytona(DaytonaConfig(api_url=API_URL, api_key=API_KEY, target="local"))
    sandbox = None
    snapshot_sandbox = None
    declarative_snapshot_sandbox = None
    snapshot_name = "daytona-python-smoke-snapshot"
    declarative_snapshot_name = "daytona-python-declarative-snapshot"

    try:
        # print("create named snapshot from declarative image")
        # snapshot_image = (
        #     Image.debian_slim("3.12")
        #     .pip_install(["numpy"])
        #     .workdir("/home/daytona")
        # )
        # built_snapshot = daytona.snapshot.create(
        #     CreateSnapshotParams(name=declarative_snapshot_name, image=snapshot_image),
        #     timeout=0,
        #     on_logs=print,
        # )
        # print(f"created named snapshot: {built_snapshot.name} state={built_snapshot.state}")

        print("create sandbox from named declarative snapshot")
        declarative_snapshot_sandbox = daytona.create(
            CreateSandboxFromSnapshotParams(snapshot=declarative_snapshot_name),
            timeout=0,
        )
        print(
            "created declarative snapshot sandbox: "
            f"{declarative_snapshot_sandbox.id} state={declarative_snapshot_sandbox.state}"
        )

        print("validate declarative snapshot sandbox")
        result = declarative_snapshot_sandbox.process.exec(
            "python -c 'import numpy; print(\"numpy-ok\")'",
            cwd="/home/daytona",
            timeout=60,
        )
        print(f"declarative snapshot check exit={result.exit_code}")
        print(result.result)

        # print("delete declarative snapshot sandbox")
        # declarative_snapshot_sandbox.delete(timeout=0)
        # declarative_snapshot_sandbox = None

        # print("define declarative image")
        # declarative_image = (
        #     Image.debian_slim("3.12")
        #     .pip_install(["requests", "pytest"])
        #     .workdir("/home/daytona")
        # )

        # print("create sandbox from declarative image")
        # sandbox = daytona.create(
        #     CreateSandboxFromImageParams(image=declarative_image),
        #     timeout=0,
        #     on_snapshot_create_logs=print,
        # )
        # print(f"created sandbox: {sandbox.id} state={sandbox.state}")
        sandbox= declarative_snapshot_sandbox
        print("validate python packages")
        result = sandbox.process.exec(
            "python -c 'import requests, pytest; print(requests.__version__); print(pytest.__version__)'",
            cwd="/home/daytona",
            timeout=60,
        )
        print(f"package check exit={result.exit_code}")
        print(result.result)

        print("validate file operations")
        sandbox.fs.create_folder("/home/daytona/smoke", "755")
        sandbox.fs.upload_file(b"hello from daytona sdk\n", "/home/daytona/smoke/hello.txt")
        downloaded = sandbox.fs.download_file("/home/daytona/smoke/hello.txt")
        print(downloaded.decode("utf-8").strip())
        print([item.path for item in sandbox.fs.list_files("/home/daytona/smoke")])

        print(f"snapshot sandbox as: {snapshot_name}")
        sandbox._experimental_create_snapshot(snapshot_name, timeout=0)

        print("pause sandbox")
        sandbox.pause(timeout=0)

        print("connect sandbox")
        sandbox = daytona.get(sandbox.id)
        print(f"connected sandbox: {sandbox.id} state={sandbox.state}")

        print("create sandbox from snapshot")
        snapshot_sandbox = daytona.create(
            CreateSandboxFromSnapshotParams(snapshot=snapshot_name),
            timeout=0,
        )
        print(f"created snapshot sandbox: {snapshot_sandbox.id} state={snapshot_sandbox.state}")

        print("validate snapshot sandbox")
        result = snapshot_sandbox.process.exec(
            "python -c 'import requests, pytest; print(\"snapshot-ok\")'",
            cwd="/home/daytona",
            timeout=60,
        )
        print(f"snapshot check exit={result.exit_code}")
        print(result.result)

        print("delete snapshot sandbox")
        snapshot_sandbox.delete(timeout=0)
        snapshot_sandbox = None

        print("delete original sandbox")
        sandbox.delete(timeout=0)
        sandbox = None

    finally:
        if declarative_snapshot_sandbox is not None:
            print("cleanup declarative snapshot sandbox")
            declarative_snapshot_sandbox.delete(timeout=0)
        if snapshot_sandbox is not None:
            print("cleanup snapshot sandbox")
            snapshot_sandbox.delete(timeout=0)
        if sandbox is not None:
            print("cleanup original sandbox")
            sandbox.delete(timeout=0)


if __name__ == "__main__":
    main()
