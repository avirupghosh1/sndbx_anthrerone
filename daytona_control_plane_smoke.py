#!/usr/bin/env python3

import os
import sys
from pathlib import Path
from urllib.parse import urlsplit


from daytona import (  # noqa: E402
    CreateSecretParams,
    CreateSandboxFromImageParams,
    CreateSandboxFromSnapshotParams,
    CreateSnapshotParams,
    Daytona,
    DaytonaConfig,
    FileDownloadRequest,
    FileUpload,
    Image,
    PtySize,
    Resources,
    SessionExecuteRequest,
    UpdateSecretParams,
)
from daytona.common.errors import DaytonaError  # noqa: E402


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
    image_sandbox = None
    pty = None
    smoke_dir = Path(__file__).resolve().parent / ".daytona_smoke_context"
    smoke_dir.mkdir(exist_ok=True)
    local_file = smoke_dir / "template-marker.txt"
    local_file.write_text("template-context-ok\n", encoding="utf-8")
    local_upload = smoke_dir / "local-upload.txt"
    local_upload.write_text("local-upload-ok\n", encoding="utf-8")
    local_download = smoke_dir / "downloaded.txt"
    snapshot_name = "daytona-smoke-snapshot"
    image_snapshot_name = "daytona-smoke-image-snapshot"

    try:
        print("build snapshot from Daytona Image")
        api_parts = urlsplit(API_URL)
        use_local_context = os.environ.get("DAYTONA_SMOKE_USE_LOCAL_CONTEXT")
        if use_local_context is None:
            use_local_context = "1" if api_parts.scheme == "https" else "0"
        if use_local_context == "1":
            print("template build context: local upload")
            image = (
                Image.debian_slim("3.12")
                .run_commands("apt-get install -y git")
                .add_local_file(local_file, "/home/daytona/template-marker.txt")
                .workdir("/home/daytona")
            )
        else:
            print("template build context: skipped for insecure local HTTP API")
            image = (
                Image.debian_slim("3.12")
                .run_commands("apt-get install -y git")
                .run_commands("mkdir -p /home/daytona && printf 'template-context-ok\\n' > /home/daytona/template-marker.txt")
                .workdir("/home/daytona")
            )
        built_snapshot = daytona.snapshot.create(
            CreateSnapshotParams(name=image_snapshot_name, image=image),
            timeout=0,
            on_logs=print,
        )
        print(f"snapshot.create: {built_snapshot.name} state={built_snapshot.state}")

        snapshots = daytona.snapshot.list(limit=20)
        print(f"snapshot.list: {len(snapshots.items)} items")
        snapshot_row = daytona.snapshot.get(image_snapshot_name)
        print(f"snapshot.get: {snapshot_row.name} state={snapshot_row.state}")

        print("organization level APIs")
        secrets = daytona.secret.list(limit=20)
        print(f"secret.list: {len(secrets.items)} items")
        volumes = daytona.volume.list()
        print(f"volume.list: {len(volumes)} items")

        print("create sandbox from built snapshot")
        sandbox = daytona.create(CreateSandboxFromSnapshotParams(snapshot=image_snapshot_name), timeout=0)
        print(f"daytona.create snapshot: {sandbox.id} state={sandbox.state}")

        sandboxes = list(daytona.list())
        print(f"daytona.list: {len(sandboxes)} sandboxes")
        sandbox = daytona.get(sandbox.id)
        print(f"daytona.get: {sandbox.id} state={sandbox.state}")
        sandbox.refresh_data()
        sandbox.refresh_activity()
        print(f"sandbox dirs: home={sandbox.get_user_home_dir()} root={sandbox.get_user_root_dir()} work={sandbox.get_work_dir()}")

        labels = sandbox.set_labels({"suite": "daytona", "kind": "compat"})
        print(f"sandbox.set_labels: {labels}")
        sandbox.set_autostop_interval(60)
        sandbox.set_auto_archive_interval(0)
        sandbox.set_auto_delete_interval(-1)
        sandbox.update_network_settings(network_block_all=False)
        preview = sandbox.get_preview_link(49983)
        print(f"sandbox.get_preview_link: {preview.url}")
        signed_preview = sandbox.create_signed_preview_url(49983, expires_in_seconds=60)
        print(f"sandbox.create_signed_preview_url: {signed_preview.url}")
        sandbox.expire_signed_preview_url(49983, signed_preview.token)
        print("sandbox.expire_signed_preview_url: ok")

        print("filesystem operations")
        marker = sandbox.fs.download_file("/home/daytona/template-marker.txt").decode("utf-8").strip()
        if marker != "template-context-ok":
            raise AssertionError(marker)
        sandbox.fs.create_folder("/home/daytona/smoke", "755")
        sandbox.fs.upload_file(b"hello from bytes\n", "/home/daytona/smoke/bytes.txt")
        sandbox.fs.upload_file(str(local_upload), "/home/daytona/smoke/local-upload.txt")
        sandbox.fs.upload_files(
            [
                FileUpload(b"bulk-a\n", "/home/daytona/smoke/bulk-a.txt"),
                FileUpload(str(local_upload), "/home/daytona/smoke/bulk-b.txt"),
            ]
        )
        print(sandbox.fs.download_file("/home/daytona/smoke/bytes.txt").decode("utf-8").strip())
        sandbox.fs.download_file("/home/daytona/smoke/local-upload.txt", str(local_download))
        if local_download.read_text(encoding="utf-8").strip() != "local-upload-ok":
            raise AssertionError("download_file to local path failed")
        bulk_download = sandbox.fs.download_files([FileDownloadRequest(source="/home/daytona/smoke/bulk-a.txt")])
        bulk_result = bulk_download[0].result if bulk_download else None
        bulk_text = bulk_result.decode("utf-8") if isinstance(bulk_result, bytes) else str(bulk_result or "")
        if not bulk_download or bulk_text.strip() != "bulk-a":
            raise AssertionError("download_files failed")
        stream_chunks = list(sandbox.fs.download_file_stream("/home/daytona/smoke/bulk-a.txt"))
        if b"".join(stream_chunks).decode("utf-8").strip() != "bulk-a":
            raise AssertionError("download_file_stream failed")
        sandbox.fs.upload_file_stream(b"stream-upload\n", "/home/daytona/smoke/stream.txt")
        if sandbox.fs.download_file("/home/daytona/smoke/stream.txt").decode("utf-8").strip() != "stream-upload":
            raise AssertionError("upload_file_stream failed")
        info = sandbox.fs.get_file_info("/home/daytona/smoke/bytes.txt")
        print(f"fs.get_file_info: {info.path} size={info.size}")
        listed = sandbox.fs.list_files("/home/daytona/smoke", depth=2)
        print(f"fs.list_files: {[item.path for item in listed]}")
        found = sandbox.fs.find_files("/home/daytona/smoke", "*.txt")
        print(f"fs.find_files: {len(found)}")
        searched = sandbox.fs.search_files("/home/daytona/smoke", "bulk")
        print(f"fs.search_files: {len(searched.files)}")
        replaced = sandbox.fs.replace_in_files(["/home/daytona/smoke/bytes.txt"], "hello", "HELLO")
        print(f"fs.replace_in_files: {len(replaced)}")
        sandbox.fs.set_file_permissions("/home/daytona/smoke/bytes.txt", "644")
        sandbox.fs.move_files("/home/daytona/smoke/bytes.txt", "/home/daytona/smoke/moved.txt")
        sandbox.fs.delete_file("/home/daytona/smoke/moved.txt")
        print("filesystem operations: ok")

        print("process operations")
        result = sandbox.process.exec("pwd && cat template-marker.txt", cwd="/home/daytona", timeout=60)
        print(f"process.exec: exit={result.exit_code} output={result.result.strip()}")
        if result.exit_code != 0 or "template-context-ok" not in result.result:
            raise AssertionError("process.exec failed")
        code_result = sandbox.process.code_run("print('code-run-ok')", timeout=60)
        print(f"process.code_run: exit={code_result.exit_code} output={code_result.result.strip()}")
        if code_result.exit_code != 0 or "code-run-ok" not in code_result.result:
            raise AssertionError("process.code_run failed")
        session_id = "smoke-session"
        sandbox.process.create_session(session_id)
        session = sandbox.process.get_session(session_id)
        print(f"process.create/get_session: {session.session_id}")
        session_result = sandbox.process.execute_session_command(
            session_id,
            SessionExecuteRequest(command="cd /home/daytona && export SMOKE_SESSION=ok && pwd"),
            timeout=60,
        )
        print(f"process.execute_session_command: {session_result.exit_code} {session_result.stdout.strip()}")
        if session_result.exit_code != 0:
            raise AssertionError("session command failed")
        session_result = sandbox.process.execute_session_command(
            session_id,
            SessionExecuteRequest(command="echo $SMOKE_SESSION"),
            timeout=60,
        )
        if session_result.stdout.strip() != "ok":
            raise AssertionError("session state was not preserved")
        command = sandbox.process.get_session_command(session_id, session_result.cmd_id)
        print(f"process.get_session_command: {command.id}")
        logs = sandbox.process.get_session_command_logs(session_id, session_result.cmd_id)
        print(f"process.get_session_command_logs: {logs.stdout.strip()}")
        sandbox.process.send_session_command_input(session_id, session_result.cmd_id, "")
        sessions = sandbox.process.list_sessions()
        print(f"process.list_sessions: {len(sessions)}")
        entrypoint = sandbox.process.get_entrypoint_session()
        print(f"process.get_entrypoint_session: {entrypoint.session_id}")
        entrypoint_logs = sandbox.process.get_entrypoint_logs()
        print(f"process.get_entrypoint_logs: {entrypoint_logs.output or ''}")
        sandbox.process.delete_session(session_id)
        print("process sessions: ok")

        print("pty operations")
        pty = sandbox.process.create_pty_session("smoke-pty", cwd="/home/daytona", pty_size=PtySize(rows=24, cols=80))
        sandbox.process.get_pty_session_info("smoke-pty")
        print(f"process.list_pty_sessions: {len(sandbox.process.list_pty_sessions())}")
        pty.resize(PtySize(rows=30, cols=100))
        pty.send_input("echo pty-ok\nexit\n")
        pty_output = []
        pty_result = pty.wait(on_data=lambda data: pty_output.append(data.decode("utf-8", "replace")), timeout=30)
        print(f"pty.wait: exit={pty_result.exit_code} output={''.join(pty_output).strip()}")
        if "pty-ok" not in "".join(pty_output):
            raise AssertionError("PTY output missing")
        pty = None
        pty = sandbox.process.create_pty_session("smoke-pty-kill", pty_size=PtySize(rows=24, cols=80))
        sandbox.process.kill_pty_session("smoke-pty-kill")
        pty = None
        print("pty operations: ok")

        print("git operations")
        repo = "/home/daytona/git-smoke"
        sandbox.process.exec(f"rm -rf {repo}")
        sandbox.git.init(repo, initial_branch="main")
        sandbox.git.configure_user("Smoke User", "smoke@example.com", scope="local", path=repo)
        sandbox.git.set_config("smoke.key", "smoke-value", scope="local", path=repo)
        if sandbox.git.get_config("smoke.key", scope="local", path=repo) != "smoke-value":
            raise AssertionError("git config failed")
        sandbox.process.exec("printf 'one\\n' > README.md", cwd=repo)
        sandbox.git.add(repo, ["README.md"])
        commit = sandbox.git.commit(repo, "initial commit", "Smoke User", "smoke@example.com")
        print(f"git.commit: {commit.sha}")
        status = sandbox.git.status(repo)
        print(f"git.status: branch={status.current_branch}")
        branches = sandbox.git.branches(repo)
        print(f"git.branches: {branches.branches}")
        sandbox.git.create_branch(repo, "feature")
        sandbox.git.checkout_branch(repo, "feature")
        sandbox.process.exec("printf 'two\\n' >> README.md", cwd=repo)
        sandbox.git.restore(repo, ["README.md"])
        sandbox.git.reset(repo)
        sandbox.git.checkout_branch(repo, "main")
        sandbox.git.delete_branch(repo, "feature")
        sandbox.git.remote_add(repo, "origin", "https://example.com/repo.git", overwrite=True)
        if sandbox.git.remote_get(repo, "origin") != "https://example.com/repo.git":
            raise AssertionError("git remote_get failed")
        remotes = sandbox.git.remotes(repo)
        print(f"git.remotes: {len(remotes.remotes)}")
        sandbox.git.dangerously_authenticate("user", "pass", host="example.com", protocol="https")
        print("git operations: ok")

        print("ssh access api methods")
        ssh_access = sandbox.create_ssh_access(expires_in_minutes=10)
        print(ssh_access.ssh_command)
        ssh_validation = sandbox.validate_ssh_access(ssh_access.token)
        if not ssh_validation.valid or ssh_validation.sandbox_id != sandbox.id:
            raise AssertionError("ssh token did not validate")
        sandbox.revoke_ssh_access(ssh_access.token)
        ssh_validation = sandbox.validate_ssh_access(ssh_access.token)
        if ssh_validation.valid:
            raise AssertionError("ssh token still validates after revoke")
        print("ssh access api methods: ok")

        print("expected unsupported methods")
        try:
            daytona.secret.create(
                CreateSecretParams(
                    name="daytona_smoke_secret",
                    value="secret-value",
                    description="daytona smoke",
                    hosts=["example.com"],
                )
            )
            raise AssertionError("secret.create unexpectedly succeeded")
        except DaytonaError as exc:
            print(f"secret.create unsupported: {exc}")
        try:
            daytona.secret.get("daytona_smoke_secret")
            raise AssertionError("secret.get unexpectedly succeeded")
        except DaytonaError as exc:
            print(f"secret.get unsupported: {exc}")
        try:
            daytona.secret.update("daytona_smoke_secret", UpdateSecretParams(value="new-value"))
            raise AssertionError("secret.update unexpectedly succeeded")
        except DaytonaError as exc:
            print(f"secret.update unsupported: {exc}")
        try:
            daytona.secret.delete("daytona_smoke_secret")
            raise AssertionError("secret.delete unexpectedly succeeded")
        except DaytonaError as exc:
            print(f"secret.delete unsupported: {exc}")
        try:
            daytona.volume.create("daytona-smoke-volume")
            raise AssertionError("volume.create unexpectedly succeeded")
        except DaytonaError as exc:
            print(f"volume.create unsupported: {exc}")
        try:
            daytona.volume.get("daytona-smoke-volume")
            raise AssertionError("volume.get unexpectedly succeeded")
        except DaytonaError as exc:
            print(f"volume.get unsupported: {exc}")
        try:
            daytona.snapshot.activate(snapshot_row)
            raise AssertionError("snapshot.activate unexpectedly succeeded")
        except DaytonaError as exc:
            print(f"snapshot.activate unsupported: {exc}")
        try:
            sandbox.archive()
            raise AssertionError("archive unexpectedly succeeded")
        except DaytonaError as exc:
            print(f"archive unsupported: {exc}")
        try:
            sandbox.recover(timeout=0)
            raise AssertionError("recover unexpectedly succeeded")
        except DaytonaError as exc:
            print(f"recover unsupported: {exc}")
        try:
            sandbox._experimental_fork(timeout=0)
            raise AssertionError("fork unexpectedly succeeded")
        except DaytonaError as exc:
            print(f"fork unsupported: {exc}")
        try:
            sandbox.resize(Resources(cpu=1, memory=1), timeout=0)
            raise AssertionError("resize unexpectedly succeeded")
        except DaytonaError as exc:
            print(f"resize unsupported: {exc}")
        try:
            sandbox.create_lsp_server("python", "/home/daytona").start()
            raise AssertionError("lsp unexpectedly succeeded")
        except DaytonaError as exc:
            print(f"lsp unsupported: {exc}")
        try:
            sandbox.code_interpreter.create_context(cwd="/home/daytona")
            raise AssertionError("code_interpreter.create_context unexpectedly succeeded")
        except DaytonaError as exc:
            print(f"code_interpreter.create_context unsupported: {exc}")
        try:
            sandbox.code_interpreter.list_contexts()
            raise AssertionError("code_interpreter.list_contexts unexpectedly succeeded")
        except DaytonaError as exc:
            print(f"code_interpreter.list_contexts unsupported: {exc}")
        try:
            sandbox.computer_use.mouse.get_position()
            raise AssertionError("computer_use.mouse unexpectedly succeeded")
        except DaytonaError as exc:
            print(f"computer_use.mouse unsupported: {exc}")
        try:
            sandbox.computer_use.screenshot.take_full_screen()
            raise AssertionError("computer_use.screenshot unexpectedly succeeded")
        except DaytonaError as exc:
            print(f"computer_use.screenshot unsupported: {exc}")
        try:
            sandbox.computer_use.start()
            raise AssertionError("computer_use.start unexpectedly succeeded")
        except DaytonaError as exc:
            print(f"computer_use.start unsupported: {exc}")

        print("create sandbox directly from Image")
        image_sandbox = daytona.create(
            CreateSandboxFromImageParams(image=Image.debian_slim("3.12").workdir("/home/daytona")),
            timeout=0,
            on_snapshot_create_logs=print,
        )
        print(f"daytona.create image: {image_sandbox.id} state={image_sandbox.state}")
        image_sandbox.delete(timeout=0)
        image_sandbox = None

        print(f"snapshot sandbox as: {snapshot_name}")
        sandbox._experimental_create_snapshot(snapshot_name, timeout=0)

        print("pause sandbox")
        sandbox.pause(timeout=0)

        print("start sandbox")
        sandbox.start(timeout=0)
        sandbox = daytona.get(sandbox.id)

        print("stop sandbox")
        sandbox.stop(timeout=0)

        print("start/connect sandbox")
        sandbox.start(timeout=0)
        sandbox = daytona.get(sandbox.id)
        print(f"connected sandbox: {sandbox.id} state={sandbox.state}")

        print("create sandbox from filesystem snapshot")
        snapshot_sandbox = daytona.create(CreateSandboxFromSnapshotParams(snapshot=snapshot_name), timeout=0)
        print(f"created snapshot sandbox: {snapshot_sandbox.id} state={snapshot_sandbox.state}")
        result = snapshot_sandbox.process.exec("test -f /home/daytona/template-marker.txt && echo snapshot-ok", timeout=60)
        if result.exit_code != 0 or "snapshot-ok" not in result.result:
            raise AssertionError("snapshot sandbox validation failed")

        print("delete snapshot sandbox")
        snapshot_sandbox.delete(timeout=0)
        snapshot_sandbox = None

        print("delete original sandbox")
        sandbox.delete(timeout=0)
        sandbox = None

        print("daytona complete smoke: ok")

    finally:
        if pty is not None:
            print("cleanup pty")
            pty.disconnect()
        if image_sandbox is not None:
            print("cleanup image sandbox")
            image_sandbox.delete(timeout=0)
        if snapshot_sandbox is not None:
            print("cleanup snapshot sandbox")
            snapshot_sandbox.delete(timeout=0)
        if sandbox is not None:
            print("cleanup original sandbox")
            sandbox.delete(timeout=0)


if __name__ == "__main__":
    main()
