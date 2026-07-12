#!/usr/bin/env python3

import os
import time

from e2b import PtySize, Sandbox, Template, wait_for_port


API_URL = os.environ.get("API_URL") or os.environ.get("E2B_API_URL")
API_KEY = os.environ.get("API_KEY") or os.environ.get("E2B_API_KEY")

if not API_URL:
    raise RuntimeError("Set API_URL or E2B_API_URL")
if not API_KEY:
    raise RuntimeError("Set API_KEY or E2B_API_KEY")

os.environ["E2B_API_URL"] = API_URL
os.environ["E2B_API_KEY"] = API_KEY
os.environ["E2B_VALIDATE_API_KEY"] = "false"

RUN_ID = os.environ.get("SMOKE_RUN_ID") or str(int(time.time()))
TEMPLATE_NAME = "e2b-python-1783"
PYTHON_VERSION = os.environ.get("PYTHON_VERSION", "3.11")
SANDBOX_TIMEOUT = int(os.environ.get("SANDBOX_TIMEOUT", "900"))
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "300"))

E2B_OPTS = {
    "api_url": API_URL,
    "api_key": API_KEY,
    "validate_api_key": False,
    "request_timeout": REQUEST_TIMEOUT,
}


def main():
    sandbox = None
    same_sandbox = None
    beta_sandbox = None
    sandbox_from_snapshot = None
    snapshot = None
    watch_handle = None
    command_handle = None
    stdin_handle = None
    direct_stdin_handle = None
    pty_kill_handle = None
    pty_handle = None
    pty_connected = None

    try:
        # print("\n=== Template methods ===")
        # print("Template source constructors: start")
        # Template().from_image(f"python:{PYTHON_VERSION}")
        # Template().from_image(f"python:{PYTHON_VERSION}", username="registry-user", password="registry-password")
        # Template().from_base_image()
        # Template().from_debian_image("bookworm")
        # Template().from_ubuntu_image("24.04")
        # Template().from_python_image(PYTHON_VERSION)
        # Template().from_node_image("20")
        # Template().from_bun_image("latest")
        # Template().from_template("base-template")
        # Template().from_aws_registry(
        #     "123456789012.dkr.ecr.us-east-1.amazonaws.com/smoke:latest",
        #     access_key_id="test",
        #     secret_access_key="test",
        #     region="us-east-1",
        # )
        # Template().from_gcp_registry(
        #     "gcr.io/smoke-project/smoke:latest",
        #     {"type": "service_account", "project_id": "smoke-project"},
        # )
        # Template().from_dockerfile(f"FROM python:{PYTHON_VERSION}\nRUN echo dockerfile-template\n")
        # print("Template source constructors: ok")

        # print("Template builder construction methods: start")
        # copy_template = (
        #     Template(file_context_path=os.path.dirname(__file__) or ".")
        #     .from_python_image(PYTHON_VERSION)
        #     .copy(
        #         "e2b_control_plane_smoke.py",
        #         "/tmp/e2b-smoke-copy.py",
        #         user="root",
        #         mode=0o644,
        #         resolve_symlinks=False,
        #     )
        #     .copy_items(
        #         [
        #             {
        #                 "src": "e2b_control_plane_smoke.py",
        #                 "dest": "/tmp/e2b-smoke-copy-items.py",
        #                 "user": "root",
        #                 "mode": 0o644,
        #                 "resolveSymlinks": False,
        #             }
        #         ]
        #     )
        #     .make_dir("/tmp/e2b-smoke-template-dir", mode=0o755, user="root")
        #     .run_cmd("echo renamed > /tmp/e2b-smoke-template-dir/source.txt", user="root")
        #     .rename(
        #         "/tmp/e2b-smoke-template-dir/source.txt",
        #         "/tmp/e2b-smoke-template-dir/renamed.txt",
        #         force=True,
        #         user="root",
        #     )
        #     .make_symlink(
        #         "/tmp/e2b-smoke-template-dir/renamed.txt",
        #         "/tmp/e2b-smoke-template-dir/link.txt",
        #         user="root",
        #         force=True,
        #     )
        #     .remove("/tmp/e2b-smoke-template-dir/link.txt", force=True, user="root")
        #     .skip_cache()
        #     .npm_install("left-pad")
        #     .bun_install("typescript", g=True)
        #     .git_clone("https://example.com/smoke.git", "/tmp/e2b-smoke-git", branch="main", depth=1)
        #     .set_start_cmd("sleep infinity", "true")
        # )
        # mcp_template = Template().from_template("mcp-gateway").add_mcp_server("filesystem").set_ready_cmd("true")
        # dockerfile_template = Template().from_dockerfile(
        #     f"FROM python:{PYTHON_VERSION}\n"
        #     "ENV E2B_SMOKE_DOCKERFILE=1\n"
        #     "WORKDIR /tmp/e2b-smoke-dockerfile\n"
        #     "RUN echo dockerfile > marker.txt\n"
        # ).set_ready_cmd("test -f /tmp/e2b-smoke-dockerfile/marker.txt")
        # Template.to_json(copy_template)
        # Template.to_dockerfile(copy_template)
        # Template.to_json(mcp_template)
        # Template.to_dockerfile(dockerfile_template)
        # print("Template copy/npm/bun/git_clone/mcp/set_start_cmd/to_json/to_dockerfile construction: ok")

        # print(f"Template.exists before build: {Template.exists(TEMPLATE_NAME, **E2B_OPTS)}")
        # template = (
        #     Template()
        #     .from_python_image(PYTHON_VERSION)
        #     .apt_install(["git"])
        #     .pip_install(["requests"])
        #     .set_envs({"E2B_SMOKE_BUILD": "1"})
        #     .set_workdir("/tmp/e2b-template-workdir")
        #     .set_user("root")
        #     .make_dir("/tmp/e2b-template-workdir")
        #     .run_cmd("echo template-built > /tmp/e2b-template-workdir/template_marker.txt")
        #     .set_ready_cmd("test -f /tmp/e2b-template-workdir/template_marker.txt")
        # )
        # build_info = Template.build(
        #     template,
        #     alias=TEMPLATE_NAME,
        #     cpu_count=2,
        #     memory_mb=1024,
        #     skip_cache=True,
        #     on_build_logs=print,
        #     **E2B_OPTS,
        # )
        # print(f"Template.build: template_id={build_info.template_id} build_id={build_info.build_id}")
        # if not Template.exists(TEMPLATE_NAME, **E2B_OPTS):
        #     raise AssertionError("Template.exists failed after build")
        # if not Template.alias_exists(TEMPLATE_NAME, **E2B_OPTS):
        #     raise AssertionError("Template.alias_exists failed after build")

        # background_template = (
        #     Template()
        #     .from_python_image(PYTHON_VERSION)
        #     .run_cmd("echo background-template-built")
        # )
        # background_build = Template.build_in_background(
        #     background_template,
        #     alias=BACKGROUND_TEMPLATE_NAME,
        #     cpu_count=1,
        #     memory_mb=512,
        #     skip_cache=True,
        #     **E2B_OPTS,
        # )
        # background_status = Template.get_build_status(background_build, logs_offset=0, **E2B_OPTS)
        # print(f"Template.build_in_background/get_build_status: {background_status.status}")
        # if str(background_status.status).lower() not in {"ready", "building"}:
        #     raise AssertionError(f"unexpected background build status: {background_status.status}")

        # assigned_tags = Template.assign_tags(TEMPLATE_NAME, ["smoke-tag"], **E2B_OPTS)
        # print(f"Template.assign_tags: {assigned_tags.tags}")
        # tags = Template.get_tags(TEMPLATE_NAME, **E2B_OPTS)
        # print(f"Template.get_tags: {[tag.tag for tag in tags]}")
        # if "smoke-tag" not in [tag.tag for tag in tags]:
        #     raise AssertionError("Template.get_tags did not include smoke-tag")
        # if not Template.exists(f"{TEMPLATE_NAME}:smoke-tag", **E2B_OPTS):
        #     raise AssertionError("tagged template alias does not exist")
        # Template.remove_tags(TEMPLATE_NAME, ["smoke-tag"], **E2B_OPTS)
        # print("Template.remove_tags: ok")
        print(f"build template: {TEMPLATE_NAME}")
        template = Template().from_python_image(PYTHON_VERSION)
        template = template.set_start_cmd("python3 -m http.server 8099", wait_for_port(8091))
        Template.build(template, alias=TEMPLATE_NAME, **E2B_OPTS)
        print("\n=== Sandbox create/list/info/timeout/metrics ===")
        sandbox = Sandbox.create(
            TEMPLATE_NAME,
            timeout=SANDBOX_TIMEOUT,
            metadata={"suite": "e2b-smoke", "run_id": RUN_ID},
            envs={"SMOKE_ENV": "ok"},
            secure=True,
            allow_internet_access=True,
            lifecycle={"on_timeout": "kill"},
            **E2B_OPTS,
        )
        print(f"Sandbox.create: {sandbox.sandbox_id}")
        print(f"sandbox domain: {getattr(sandbox, 'sandbox_domain', '')}")
        if not sandbox.is_running(request_timeout=REQUEST_TIMEOUT):
            raise AssertionError("sandbox.is_running returned false")

        same_sandbox = sandbox.connect(**E2B_OPTS)
        print(f"sandbox.connect instance: {same_sandbox.sandbox_id}")
        if same_sandbox.sandbox_id != sandbox.sandbox_id:
            raise AssertionError("instance connect returned a different sandbox")

        sandbox_list = Sandbox.list(limit=100, **E2B_OPTS)
        sandbox_items = sandbox_list.next_items(**E2B_OPTS)
        print(f"Sandbox.list: count={len(sandbox_items)} has_next={sandbox_list.has_next}")
        if sandbox.sandbox_id not in [item.sandbox_id for item in sandbox_items]:
            raise AssertionError("Sandbox.list did not include created sandbox")

        info = sandbox.get_info(**E2B_OPTS)
        class_info = Sandbox.get_info(sandbox.sandbox_id, **E2B_OPTS)
        print(f"sandbox.get_info: {info.sandbox_id}")
        print(f"Sandbox.get_info: {class_info.sandbox_id}")
        if info.sandbox_id != sandbox.sandbox_id or class_info.sandbox_id != sandbox.sandbox_id:
            raise AssertionError("get_info returned wrong sandbox id")

        print("\n=== Sandbox URL and network methods ===")
        host_8080 = sandbox.get_host(8080)
        mcp_url = sandbox.get_mcp_url()
        download_url = sandbox.download_url("/tmp/e2b-smoke/fs/hello.txt")
        upload_url = sandbox.upload_url("/tmp/e2b-smoke/fs/uploaded.txt")
        print(f"sandbox.get_host: {host_8080}")
        print(f"sandbox.get_mcp_url: {mcp_url}")
        print(f"sandbox.download_url: {download_url}")
        print(f"sandbox.upload_url: {upload_url}")
        if not host_8080 or not mcp_url.endswith("/mcp"):
            raise AssertionError("sandbox URL helpers returned unexpected URLs")
        if "/files" not in download_url or "/files" not in upload_url:
            raise AssertionError("sandbox file URL helpers returned unexpected URLs")
        try:
            sandbox.update_network({"allow_internet_access": True}, **E2B_OPTS)
        except Exception as exc:
            if "not implemented" not in str(exc).lower() and "501" not in str(exc):
                raise
            print("sandbox.update_network instance: not implemented as expected")
        else:
            raise AssertionError("sandbox.update_network unexpectedly succeeded")
        try:
            Sandbox.update_network(sandbox.sandbox_id, {"allow_internet_access": True}, **E2B_OPTS)
        except Exception as exc:
            if "not implemented" not in str(exc).lower() and "501" not in str(exc):
                raise
            print("Sandbox.update_network class: not implemented as expected")
        else:
            raise AssertionError("Sandbox.update_network unexpectedly succeeded")

        print("\n=== Sandbox timeout/metrics methods ===")
        sandbox.set_timeout(SANDBOX_TIMEOUT, **E2B_OPTS)
        Sandbox.set_timeout(sandbox.sandbox_id, SANDBOX_TIMEOUT, **E2B_OPTS)
        print("sandbox.set_timeout / Sandbox.set_timeout: ok")

        metrics = sandbox.get_metrics(**E2B_OPTS)
        class_metrics = Sandbox.get_metrics(sandbox.sandbox_id, **E2B_OPTS)
        print(f"sandbox.get_metrics: {len(metrics)}")
        print(f"Sandbox.get_metrics: {len(class_metrics)}")

        if hasattr(Sandbox, "beta_create"):
            beta_sandbox = Sandbox.beta_create(
                TEMPLATE_NAME,
                timeout=SANDBOX_TIMEOUT,
                auto_pause=True,
                metadata={"suite": "e2b-smoke-beta", "run_id": RUN_ID},
                envs={"SMOKE_ENV": "beta"},
                secure=True,
                allow_internet_access=True,
                **E2B_OPTS,
            )
            print(f"Sandbox.beta_create: {beta_sandbox.sandbox_id}")
        else:
            print("Sandbox.beta_create: not present in this SDK version")

        print("\n=== Filesystem methods ===")
        file_path = "/tmp/e2b-smoke/fs/hello.txt"
        renamed_path = "/tmp/e2b-smoke/fs/renamed.txt"
        binary_path = "/tmp/e2b-smoke/fs/binary.bin"
        watch_dir = "/tmp/e2b-smoke/watch"
        if not sandbox.files.make_dir("/tmp/e2b-smoke/fs"):
            print("files.make_dir: directory already existed")
        sandbox.files.write(file_path, "hello from e2b smoke")
        if sandbox.files.read(file_path) != "hello from e2b smoke":
            raise AssertionError("files.read text mismatch")
        if bytes(sandbox.files.read(file_path, format="bytes")) != b"hello from e2b smoke":
            raise AssertionError("files.read bytes mismatch")
        with sandbox.files.read(file_path, format="stream") as stream:
            if b"".join(stream) != b"hello from e2b smoke":
                raise AssertionError("files.read stream mismatch")
        writes = sandbox.files.write_files(
            [
                {"path": binary_path, "data": b"\x00\x01\x02"},
                {"path": "/tmp/e2b-smoke/fs/second.txt", "data": "second"},
            ]
        )
        print(f"files.write_files: {[item.path for item in writes]}")
        listed = sandbox.files.list("/tmp/e2b-smoke/fs", depth=1)
        print(f"files.list: {[entry.name for entry in listed]}")
        if not sandbox.files.exists(file_path):
            raise AssertionError("files.exists returned false")
        file_info = sandbox.files.get_info(file_path)
        print(f"files.get_info: {file_info.path}")
        moved_info = sandbox.files.rename(file_path, renamed_path)
        print(f"files.rename: {moved_info.path}")
        sandbox.files.remove(renamed_path)
        if sandbox.files.exists(renamed_path):
            raise AssertionError("files.remove did not remove file")
        sandbox.files.make_dir(watch_dir)
        watch_handle = sandbox.files.watch_dir(watch_dir, recursive=True, include_entry=True)
        sandbox.files.write(f"{watch_dir}/watched.txt", "watched")
        events = watch_handle.get_new_events()
        if not events:
            time.sleep(0.5)
            events = watch_handle.get_new_events()
        print(f"watch_handle.get_new_events: {[(event.name, event.type.value) for event in events]}")
        if not events:
            raise AssertionError("watch_handle.get_new_events returned no events")
        watch_handle.stop()
        watch_handle = None
        print("watch_handle.stop: ok")

        print("\n=== Command methods ===")
        stdout_chunks = []
        stderr_chunks = []
        command_result = sandbox.commands.run(
            "printf stdout && printf stderr >&2",
            envs={"SMOKE_COMMAND_ENV": "ok"},
            cwd="/tmp",
            on_stdout=stdout_chunks.append,
            on_stderr=stderr_chunks.append,
            timeout=60,
        )
        print(f"commands.run foreground: stdout={command_result.stdout!r} stderr={command_result.stderr!r}")
        if command_result.stdout != "stdout" or command_result.stderr != "stderr":
            raise AssertionError("commands.run foreground output mismatch")

        stdin_handle = sandbox.commands.run("cat", background=True, stdin=True, timeout=60)
        sandbox.commands.send_stdin(stdin_handle.pid, "stdin via commands\n")
        stdin_handle.send_stdin("stdin via handle\n")
        stdin_handle.close_stdin()
        stdin_result = stdin_handle.wait()
        print(f"commands.send_stdin / handle.send_stdin / handle.wait: {stdin_result.stdout!r}")
        if "stdin via commands" not in stdin_result.stdout or "stdin via handle" not in stdin_result.stdout:
            raise AssertionError("stdin command output mismatch")
        stdin_handle = None

        direct_stdin_handle = sandbox.commands.run("cat", background=True, stdin=True, timeout=60)
        sandbox.commands.send_stdin(direct_stdin_handle.pid, "stdin then direct close\n")
        sandbox.commands.close_stdin(direct_stdin_handle.pid)
        direct_stdin_result = direct_stdin_handle.wait()
        print(f"commands.close_stdin: {direct_stdin_result.stdout!r}")
        if "stdin then direct close" not in direct_stdin_result.stdout:
            raise AssertionError("commands.close_stdin output mismatch")
        direct_stdin_handle = None

        iter_handle = sandbox.commands.run("printf iter-output", background=True, timeout=60)
        iter_stdout = []
        iter_stderr = []
        iter_pty = []
        for stdout, stderr, pty in iter_handle:
            if stdout is not None:
                iter_stdout.append(stdout)
            if stderr is not None:
                iter_stderr.append(stderr)
            if pty is not None:
                iter_pty.append(pty)
        print(f"CommandHandle.__iter__: stdout={''.join(iter_stdout)!r}")
        if "".join(iter_stdout) != "iter-output" or iter_stderr or iter_pty:
            raise AssertionError("CommandHandle.__iter__ output mismatch")

        command_handle = sandbox.commands.run("sleep 30", background=True, timeout=0)
        command_pid = command_handle.pid
        running = sandbox.commands.list()
        print(f"commands.list: {[proc.pid for proc in running]}")
        if command_pid not in [proc.pid for proc in running]:
            raise AssertionError("commands.list missing background pid")
        command_handle.disconnect()
        connected_handle = sandbox.commands.connect(command_pid, timeout=1)
        print(f"commands.connect: {connected_handle.pid}")
        if connected_handle.pid != command_pid:
            raise AssertionError("commands.connect returned wrong pid")
        if not sandbox.commands.kill(command_pid):
            raise AssertionError("commands.kill returned false")
        try:
            connected_handle.wait()
        except Exception as exc:
            print(f"connected command wait after kill: {type(exc).__name__}")
        command_handle = None

        kill_handle = sandbox.commands.run("sleep 30", background=True, timeout=0)
        if not kill_handle.kill():
            raise AssertionError("CommandHandle.kill returned false")
        print("CommandHandle.kill: ok")
        if sandbox.commands.kill(99999999):
            raise AssertionError("commands.kill missing pid returned true")
        print("commands.kill missing pid: false")

        print("\n=== PTY methods ===")
        pty_handle = sandbox.pty.create(PtySize(rows=24, cols=80), cwd="/tmp", timeout=0)
        pty_pid = pty_handle.pid
        pty_handle.disconnect()
        pty_connected = sandbox.pty.connect(pty_pid, timeout=5)
        sandbox.pty.resize(pty_pid, PtySize(rows=30, cols=100))
        sandbox.pty.send_stdin(pty_pid, b"echo pty-ok\nexit\n")
        pty_result = pty_connected.wait()
        print(f"pty.create/connect/send_stdin/resize/wait: exit={pty_result.exit_code}")
        if pty_result.exit_code != 0:
            raise AssertionError("PTY exited non-zero")
        pty_handle = None
        pty_connected = None
        pty_kill_handle = sandbox.pty.create(PtySize(rows=24, cols=80), cwd="/tmp", timeout=0)
        if not sandbox.pty.kill(pty_kill_handle.pid):
            raise AssertionError("pty.kill real pid returned false")
        print("pty.kill real pid: ok")
        pty_kill_handle = None
        if sandbox.pty.kill(99999999):
            raise AssertionError("pty.kill missing pid returned true")
        print("pty.kill missing pid: false")

        print("\n=== Git methods ===")
        sandbox.commands.run("rm -rf /tmp/e2b-git /tmp/e2b-git-remote /tmp/e2b-git-clone")
        sandbox.git.init("/tmp/e2b-git-remote", bare=True, initial_branch="main")
        sandbox.git.init("/tmp/e2b-git", initial_branch="main")
        sandbox.git.configure_user("E2B Smoke", "smoke@example.com", scope="local", path="/tmp/e2b-git")
        sandbox.files.write("/tmp/e2b-git/README.md", "hello git\n")
        git_status = sandbox.git.status("/tmp/e2b-git")
        print(f"git.status before add: changes={git_status.total_count}")
        if not git_status.has_untracked:
            raise AssertionError("git.status did not detect untracked file")
        sandbox.git.add("/tmp/e2b-git", files=["README.md"])
        sandbox.git.commit("/tmp/e2b-git", "initial commit")
        branches = sandbox.git.branches("/tmp/e2b-git")
        print(f"git.branches: {branches.branches} current={branches.current_branch}")
        if "main" not in branches.branches:
            raise AssertionError("git.branches missing main")
        sandbox.git.remote_add("/tmp/e2b-git", "origin", "/tmp/e2b-git-remote", overwrite=True)
        if sandbox.git.remote_get("/tmp/e2b-git", "origin") != "/tmp/e2b-git-remote":
            raise AssertionError("git.remote_get mismatch")
        sandbox.git.push("/tmp/e2b-git", remote="origin", branch="main", set_upstream=True)
        sandbox.git.clone("/tmp/e2b-git-remote", "/tmp/e2b-git-clone")
        sandbox.git.configure_user("E2B Smoke", "smoke@example.com", scope="local", path="/tmp/e2b-git-clone")
        sandbox.git.pull("/tmp/e2b-git-clone", remote="origin", branch="main")
        sandbox.git.set_config("smoke.key", "smoke-value", scope="local", path="/tmp/e2b-git")
        if sandbox.git.get_config("smoke.key", scope="local", path="/tmp/e2b-git") != "smoke-value":
            raise AssertionError("git.get_config mismatch")
        sandbox.git.create_branch("/tmp/e2b-git", "feature")
        sandbox.files.write("/tmp/e2b-git/README.md", "changed\n")
        sandbox.git.add("/tmp/e2b-git", files=["README.md"])
        staged_status = sandbox.git.status("/tmp/e2b-git")
        if not staged_status.has_staged:
            raise AssertionError("git.add did not stage README.md")
        sandbox.git.reset("/tmp/e2b-git", paths=["README.md"])
        sandbox.git.restore("/tmp/e2b-git", paths=["README.md"], worktree=True)
        if sandbox.files.read("/tmp/e2b-git/README.md") != "hello git\n":
            raise AssertionError("git.restore did not restore README.md")
        sandbox.git.checkout_branch("/tmp/e2b-git", "main")
        sandbox.git.delete_branch("/tmp/e2b-git", "feature", force=True)
        sandbox.git.dangerously_authenticate("smoke-user", "smoke-password", host="example.com")
        print("git clone/init/status/branches/branch/add/commit/reset/restore/push/pull/remotes/config/auth: ok")

        print("\n=== MCP token method ===")
        sandbox.files.make_dir("/etc/mcp-gateway", user="root")
        sandbox.files.write("/etc/mcp-gateway/.token", "local-mcp-token", user="root")
        if sandbox.get_mcp_token() != "local-mcp-token":
            raise AssertionError("sandbox.get_mcp_token mismatch")
        print("sandbox.get_mcp_token: ok")

        print("\n=== Snapshot methods ===")
        snapshot = sandbox.create_snapshot("e2b-smoke-snapshot", **E2B_OPTS)
        print(f"sandbox.create_snapshot: {snapshot.snapshot_id}")
        instance_snapshots = sandbox.list_snapshots(limit=100, **E2B_OPTS)
        instance_snapshot_items = instance_snapshots.next_items(**E2B_OPTS)
        class_snapshots = Sandbox.list_snapshots(sandbox.sandbox_id, limit=100, **E2B_OPTS)
        class_snapshot_items = class_snapshots.next_items(**E2B_OPTS)
        print(f"sandbox.list_snapshots: {[item.snapshot_id for item in instance_snapshot_items]}")
        print(f"Sandbox.list_snapshots: {[item.snapshot_id for item in class_snapshot_items]}")
        if snapshot.snapshot_id not in [item.snapshot_id for item in instance_snapshot_items]:
            raise AssertionError("instance list_snapshots missing snapshot")
        if snapshot.snapshot_id not in [item.snapshot_id for item in class_snapshot_items]:
            raise AssertionError("class list_snapshots missing snapshot")

        sandbox_from_snapshot = Sandbox.create(
            snapshot.snapshot_id,
            timeout=SANDBOX_TIMEOUT,
            metadata={"suite": "e2b-smoke-snapshot", "run_id": RUN_ID},
            **E2B_OPTS,
        )
        print(f"Sandbox.create from snapshot: {sandbox_from_snapshot.sandbox_id}")
        if not sandbox_from_snapshot.is_running(request_timeout=REQUEST_TIMEOUT):
            raise AssertionError("snapshot sandbox is not running")

        print("\n=== Pause/connect/kill methods ===")
        sandbox_from_snapshot.pause(**E2B_OPTS)
        print("sandbox.pause instance: ok")
        sandbox_from_snapshot = Sandbox.connect(sandbox_from_snapshot.sandbox_id, timeout=SANDBOX_TIMEOUT, **E2B_OPTS)
        print(f"Sandbox.connect class: {sandbox_from_snapshot.sandbox_id}")
        Sandbox.pause(sandbox_from_snapshot.sandbox_id, **E2B_OPTS)
        print("Sandbox.pause class: ok")
        sandbox_from_snapshot = sandbox_from_snapshot.connect(**E2B_OPTS)
        print(f"sandbox.connect instance after class pause: {sandbox_from_snapshot.sandbox_id}")
        if hasattr(Sandbox, "beta_pause"):
            Sandbox.beta_pause(sandbox_from_snapshot.sandbox_id, **E2B_OPTS)
            print("Sandbox.beta_pause class: ok")
            sandbox_from_snapshot = Sandbox.connect(sandbox_from_snapshot.sandbox_id, timeout=SANDBOX_TIMEOUT, **E2B_OPTS)
        else:
            print("Sandbox.beta_pause: not present in this SDK version")

        if not Sandbox.kill(sandbox_from_snapshot.sandbox_id, **E2B_OPTS):
            raise AssertionError("Sandbox.kill class returned false")
        print("Sandbox.kill class: ok")
        sandbox_from_snapshot = None

        if not Sandbox.delete_snapshot(snapshot.snapshot_id, **E2B_OPTS):
            raise AssertionError("Sandbox.delete_snapshot returned false")
        print("Sandbox.delete_snapshot: ok")
        snapshot = None

        sandbox.pause(**E2B_OPTS)
        print("sandbox.pause original: ok")
        sandbox = Sandbox.connect(sandbox.sandbox_id, timeout=SANDBOX_TIMEOUT, **E2B_OPTS)
        print("Sandbox.connect original after pause: ok")

        if not sandbox.kill(**E2B_OPTS):
            raise AssertionError("sandbox.kill instance returned false")
        print("sandbox.kill instance: ok")
        sandbox = None

        if beta_sandbox is not None:
            beta_sandbox.kill(**E2B_OPTS)
            beta_sandbox = None

        print("\nE2B smoke completed")

    finally:
        if watch_handle is not None:
            print("cleanup watch handle")
            watch_handle.stop()
        if pty_connected is not None:
            print("cleanup connected pty")
            pty_connected.kill()
        if pty_handle is not None:
            print("cleanup pty")
            pty_handle.kill()
        if stdin_handle is not None:
            print("cleanup stdin command")
            stdin_handle.kill()
        if direct_stdin_handle is not None:
            print("cleanup direct stdin command")
            direct_stdin_handle.kill()
        if command_handle is not None:
            print("cleanup command")
            command_handle.kill()
        if pty_kill_handle is not None:
            print("cleanup pty kill handle")
            pty_kill_handle.kill()
        if sandbox_from_snapshot is not None:
            print("cleanup snapshot sandbox")
            sandbox_from_snapshot.kill(**E2B_OPTS)
        if beta_sandbox is not None:
            print("cleanup beta sandbox")
            beta_sandbox.kill(**E2B_OPTS)
        if sandbox is not None:
            print("cleanup original sandbox")
            sandbox.kill(**E2B_OPTS)
        if snapshot is not None:
            print("cleanup snapshot record")
            Sandbox.delete_snapshot(snapshot.snapshot_id, **E2B_OPTS)


if __name__ == "__main__":
    main()
