#!/usr/bin/env python3

import os
import sys
import time
from pathlib import Path


# DEFAULT_MODAL_CLIENT_DIR = Path(__file__).resolve().parent.parent / "modal-client" / "py"
# MODAL_CLIENT_DIR = os.environ.get("MODAL_CLIENT_DIR", "")
# if MODAL_CLIENT_DIR:
#     sys.path.insert(0, MODAL_CLIENT_DIR)
# elif (DEFAULT_MODAL_CLIENT_DIR / "modal_proto" / "api_pb2.py").exists():
#     sys.path.insert(0, str(DEFAULT_MODAL_CLIENT_DIR))

API_URL = os.environ.get("MODAL_SERVER_URL") or os.environ.get("API_URL")
API_KEY = os.environ.get("MODAL_TOKEN_SECRET") or os.environ.get("API_KEY")

if not API_URL:
    raise RuntimeError("Set MODAL_SERVER_URL or API_URL, for example http://127.0.0.1:50051")
if not API_KEY:
    raise RuntimeError("Set MODAL_TOKEN_SECRET or API_KEY")

os.environ["MODAL_SERVER_URL"] = API_URL
os.environ["MODAL_TOKEN_ID"] = os.environ.get("MODAL_TOKEN_ID", "sndbx-local")
os.environ["MODAL_TOKEN_SECRET"] = API_KEY
os.environ.setdefault("MODAL_USE_LEGACY_FILESYSTEM_SNAPSHOT", "1")

import modal  # noqa: E402


def main():
    run_id = os.environ.get("SMOKE_RUN_ID") or str(int(time.time()))
    app_name = os.environ.get("MODAL_SMOKE_APP", f"modal-compat-smoke-{run_id}")
    image_name = os.environ.get("MODAL_SMOKE_IMAGE", f"modal-compat-image-{run_id}:latest")
    sandbox_name = os.environ.get("MODAL_SMOKE_SANDBOX", f"modal-smoke-{run_id}")
    v2_sandbox_name = os.environ.get("MODAL_SMOKE_V2_SANDBOX", f"modal-v2-smoke-{run_id}")

    sandbox = None
    restored = None
    fs_sandbox = None
    v2_sandbox = None

    try:
        print("lookup app")
        app = modal.App.lookup(app_name, create_if_missing=True)
        print(f"app: {app.app_id}")

        print("build image")
        image = (
            modal.Image.debian_slim("3.12")
            .run_commands("mkdir -p /tmp/modal-smoke && printf 'image-ok\\n' > /tmp/modal-smoke/image.txt")
            .workdir("/tmp/modal-smoke")
        )
        image = image.build(app)
        print(f"image.build: {image.object_id}")

        print("publish and resolve image tag")
        image.publish(image_name)
        named_image = modal.Image.from_name(image_name)
        print("image.publish/from_name: ok")

        print("create v1 sandbox")
        sandbox = modal.Sandbox.create(
            "sleep",
            "300",
            app=app,
            image=named_image,
            name=sandbox_name,
            tags={"suite": "modal-smoke", "run_id": run_id},
            timeout=300,
            cpu=1.0,
            memory=512,
            workdir="/tmp/modal-smoke",
            unencrypted_ports=[8000],
        )
        print(f"Sandbox.create: {sandbox.object_id}")

        print("sandbox lookup/list/tag/control methods")
        same = modal.Sandbox.from_id(sandbox.object_id)
        by_name = modal.Sandbox.from_name(app_name, sandbox_name)
        if same.object_id != sandbox.object_id or by_name.object_id != sandbox.object_id:
            raise AssertionError("sandbox lookup returned wrong id")
        listed_ids = [item.object_id for item in modal.Sandbox.list()]
        if sandbox.object_id not in listed_ids:
            raise AssertionError("Sandbox.list did not include created sandbox")
        sandbox.set_tags({"suite": "modal-smoke", "updated": "true"})
        tags = sandbox.get_tags()
        if tags.get("updated") != "true":
            raise AssertionError(f"Sandbox.get_tags mismatch: {tags}")
        if sandbox.poll() is not None:
            raise AssertionError("Sandbox.poll returned finished status too early")
        sandbox.wait_until_ready(timeout=30)
        tunnels = sandbox.tunnels(timeout=5)
        print(f"Sandbox.tunnels: {sorted(tunnels)}")
        creds = sandbox.create_connect_token(port=8000)
        if not creds.url or not creds.token:
            raise AssertionError("Sandbox.create_connect_token returned empty credentials")
        print(f"Sandbox.create_connect_token: {creds.url}")
        sandbox.reload_volumes(timeout=30)
        print("Sandbox.reload_volumes: ok")

        print("exec command")
        proc = sandbox.exec("sh", "-lc", "cat image.txt && echo exec-ok", workdir="/tmp/modal-smoke", timeout=30)
        stdout = proc.stdout.read()
        stderr = proc.stderr.read()
        code = proc.wait()
        print(stdout.strip())
        if stderr:
            print(stderr.strip())
        if code != 0 or "image-ok" not in stdout or "exec-ok" not in stdout:
            raise AssertionError(f"exec failed code={code} stdout={stdout!r} stderr={stderr!r}")

        print("deprecated control-plane file methods")
        sandbox.mkdir("/tmp/modal-smoke/files", parents=True)
        writer = sandbox.open("/tmp/modal-smoke/files/hello.txt", "w")
        writer.write("hello from modal file api\n")
        writer.close()
        reader = sandbox.open("/tmp/modal-smoke/files/hello.txt", "r")
        file_text = reader.read()
        reader.close()
        if file_text != "hello from modal file api\n":
            raise AssertionError(f"file api read mismatch: {file_text!r}")
        names = sandbox.ls("/tmp/modal-smoke/files")
        if "hello.txt" not in names:
            raise AssertionError(f"file api ls mismatch: {names!r}")
        sandbox.rm("/tmp/modal-smoke/files/hello.txt")
        print("Sandbox.open/ls/mkdir/rm: ok")

        print("snapshot and restore")
        snapshot = sandbox._experimental_snapshot()
        restored = modal.Sandbox._experimental_from_snapshot(snapshot, name=f"{sandbox_name}-restored")
        restored_proc = restored.exec("sh", "-lc", "echo restored-ok", timeout=30)
        restored_stdout = restored_proc.stdout.read()
        restored_code = restored_proc.wait()
        if restored_code != 0 or "restored-ok" not in restored_stdout:
            raise AssertionError(f"restored exec failed: {restored_stdout!r}")
        print("Sandbox._experimental_snapshot/from_snapshot: ok")

        print("legacy filesystem snapshot image")
        fs_image = sandbox.snapshot_filesystem(timeout=55)
        fs_sandbox = modal.Sandbox.create(
            "sh",
            "-lc",
            "cat /tmp/modal-smoke/image.txt && sleep 5",
            app=app,
            image=fs_image,
            timeout=120,
        )
        fs_proc = fs_sandbox.exec("sh", "-lc", "cat /tmp/modal-smoke/image.txt", timeout=30)
        fs_stdout = fs_proc.stdout.read()
        fs_code = fs_proc.wait()
        if fs_code != 0 or "image-ok" not in fs_stdout:
            raise AssertionError(f"snapshot filesystem image failed: {fs_stdout!r}")
        print("Sandbox.snapshot_filesystem: ok")

        print("create v2 sandbox")
        v2_sandbox = modal.Sandbox._experimental_create(
            "sleep",
            "300",
            app=app,
            image=image,
            name=v2_sandbox_name,
            tags={"suite": "modal-smoke-v2", "run_id": run_id},
            timeout=300,
            cpu=1.0,
            memory=512,
            workdir="/tmp/modal-smoke",
        )
        print(f"Sandbox._experimental_create: {v2_sandbox.object_id}")
        v2_by_name = modal.Sandbox._experimental_from_name(app_name, v2_sandbox_name)
        if v2_by_name.object_id != v2_sandbox.object_id:
            raise AssertionError("Sandbox._experimental_from_name returned wrong id")
        v2_ids = [item.object_id for item in modal.Sandbox._experimental_list(app_id=app.app_id)]
        if v2_sandbox.object_id not in v2_ids:
            raise AssertionError("Sandbox._experimental_list did not include created sandbox")
        v2_proc = v2_sandbox.exec("sh", "-lc", "cat image.txt && echo v2-exec-ok", workdir="/tmp/modal-smoke", timeout=30)
        v2_stdout = v2_proc.stdout.read()
        v2_code = v2_proc.wait()
        print(v2_stdout.strip())
        if v2_code != 0 or "v2-exec-ok" not in v2_stdout:
            raise AssertionError(f"v2 exec failed: {v2_stdout!r}")

    finally:
        print("cleanup")
        for item in (v2_sandbox, fs_sandbox, restored, sandbox):
            if item is not None:
                try:
                    item.terminate(wait=True)
                except Exception as exc:
                    print(f"cleanup warning: {exc}")


if __name__ == "__main__":
    main()
