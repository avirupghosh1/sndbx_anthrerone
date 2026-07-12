#!/usr/bin/env python3

import os
import time

import e2b
from e2b import Sandbox, Template, wait_for_port


API_URL = os.environ.get("API_URL") or os.environ.get("E2B_API_URL")
API_KEY = os.environ.get("API_KEY") or os.environ.get("E2B_API_KEY")

if not API_URL:
    raise RuntimeError("Set API_URL or E2B_API_URL")
if not API_KEY:
    raise RuntimeError("Set API_KEY or E2B_API_KEY")

os.environ["E2B_API_URL"] = API_URL
os.environ["E2B_API_KEY"] = API_KEY
os.environ["E2B_VALIDATE_API_KEY"] = "false"

TEMPLATE_NAME = "e2b-python-1783617759"
PYTHON_VERSION = os.environ.get("PYTHON_VERSION", "3.11")
SANDBOX_TIMEOUT = int(os.environ.get("SANDBOX_TIMEOUT", "600"))
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "300"))

E2B_OPTS = {
    "api_url": API_URL,
    "api_key": API_KEY,
    "validate_api_key": False,
    "request_timeout": REQUEST_TIMEOUT,
}


def main():
    sandbox = None
    sandbox_from_snapshot = None

    try:
        print(f"build template: {TEMPLATE_NAME}")
        template = Template().from_python_image(PYTHON_VERSION)
        template = template.set_start_cmd("python3 -m http.server 8099", wait_for_port(8091))
        Template.build(template, alias=TEMPLATE_NAME, **E2B_OPTS)

        print(f"create sandbox from template: {TEMPLATE_NAME}")
        sandbox = Sandbox.create(TEMPLATE_NAME, timeout=SANDBOX_TIMEOUT, **E2B_OPTS)
        print(f"created sandbox: {sandbox.sandbox_id}")
        print(f"sandbox domain: {getattr(sandbox, 'sandbox_domain', '')}")
        content = "Hello from E2B!\nThis is a test file."
        path = "/tmp/test_file.txt"

        # Write the file
        sandbox.files.write(path, content)

        # Read it back
        read_content = sandbox.files.read(path)
        print(read_content)
        # Verify
        if read_content == content:
            print("✅ Content matches!")
        print("snapshot sandbox")
        snapshot = sandbox.create_snapshot(**E2B_OPTS)
        print(f"snapshot id: {snapshot.snapshot_id}")

        print(f"create sandbox from snapshot: {snapshot.snapshot_id}")
        sandbox_from_snapshot = Sandbox.create(
            snapshot.snapshot_id,
            timeout=SANDBOX_TIMEOUT,
            **E2B_OPTS,
        )
        print(f"created snapshot sandbox: {sandbox_from_snapshot.sandbox_id}")
        print(
            "snapshot sandbox domain: "
            f"{getattr(sandbox_from_snapshot, 'sandbox_domain', '')}"
        )

        print("pause snapshot sandbox")
        sandbox_from_snapshot.pause(**E2B_OPTS)

        print("reconnect snapshot sandbox")
        sandbox_from_snapshot = Sandbox.connect(
            sandbox_from_snapshot.sandbox_id,
            **E2B_OPTS,
        )
        print(f"reconnected sandbox: {sandbox_from_snapshot.sandbox_id}")

        print("kill snapshot sandbox")
        sandbox_from_snapshot.kill(**E2B_OPTS)
        sandbox_from_snapshot = None

        print("kill original sandbox")
        sandbox.kill(**E2B_OPTS)
        sandbox = None

    finally:
        if sandbox_from_snapshot is not None:
            print("cleanup snapshot sandbox")
            sandbox_from_snapshot.kill(**E2B_OPTS)
        if sandbox is not None:
            print("cleanup original sandbox")
            sandbox.kill(**E2B_OPTS)


if __name__ == "__main__":
    main()
