"""Static content for the embedded documentation portal."""

from __future__ import annotations

import re
from typing import Any


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "section"


def _p(text: str) -> dict[str, Any]:
    return {"type": "paragraph", "text": text}


def _h(title: str) -> dict[str, Any]:
    return {"type": "heading", "title": title, "id": _slugify(title)}


def _list(items: list[str]) -> dict[str, Any]:
    return {"type": "list", "items": items}


def _code(title: str, value: str, language: str = "bash") -> dict[str, Any]:
    return {
        "type": "code",
        "title": title,
        "language": language,
        "code": value.strip(),
    }


def _cards(cards: list[dict[str, str]]) -> dict[str, Any]:
    return {"type": "cards", "cards": cards}


def _table(headers: list[str], rows: list[list[str]]) -> dict[str, Any]:
    return {"type": "table", "headers": headers, "rows": rows}


def _methods(methods: list[dict[str, str]]) -> dict[str, Any]:
    return {"type": "methods", "methods": methods}


def _endpoint(
    method: str,
    path: str,
    summary: str,
    *,
    auth: str = "X-API-Key or Authorization: Bearer",
    request: str = "",
    response: str = "",
    example: str = "",
    href: str = "",
) -> dict[str, str]:
    return {
        "method": method,
        "path": path,
        "summary": summary,
        "auth": auth,
        "request": request,
        "response": response,
        "example": example.strip(),
        "href": href,
    }


def _endpoints(title: str, items: list[dict[str, str]]) -> dict[str, Any]:
    return {
        "type": "endpoints",
        "title": title,
        "id": _slugify(title),
        "items": items,
    }


def _page(
    section: str,
    slug: str,
    title: str,
    description: str,
    group: str,
    blocks: list[dict[str, Any]],
    badges: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "section": section,
        "slug": slug,
        "title": title,
        "description": description,
        "group": group,
        "badges": badges or [],
        "blocks": blocks,
        "href": f"/docs/{section}/{slug}",
    }


SECTIONS: list[dict[str, str]] = [
    {
        "id": "documentation",
        "label": "Documentation",
        "description": "Guides for running sandboxes, templates, files, commands, and operators.",
        "default": "overview",
    },
    {
        "id": "sdk-reference",
        "label": "SDK Reference",
        "description": "Python SDK classes and helpers exposed by my_sandbox_sdk.",
        "default": "overview",
    },
    {
        "id": "api-reference",
        "label": "API Reference",
        "description": "HTTP endpoints exposed by api-service, including compatibility routes.",
        "default": "auth/create-access-token",
    },
]


PAGES: list[dict[str, Any]] = [
    _page(
        "documentation",
        "overview",
        "Overview",
        "SNDBX exposes an E2B-shaped sandbox platform with a generic local API, Python SDK, template builds, compatibility layers, and an operator portal.",
        "Start",
        [
            _h("What this portal covers"),
            _cards(
                [
                    {
                        "title": "Documentation",
                        "text": "Task-focused guides for creating sandboxes, building templates, moving files, opening ports, and observing runtime state.",
                        "href": "/docs/documentation/quickstart",
                    },
                    {
                        "title": "SDK Reference",
                        "text": "Python SDK pages for Sandbox, Commands, Filesystem, Template, errors, logging, process, PTY, Git, and data models.",
                        "href": "/docs/sdk-reference/overview",
                    },
                    {
                        "title": "API Reference",
                        "text": "Core REST endpoints plus E2B and Daytona compatibility endpoints grouped separately from admin and internal surfaces.",
                        "href": "/docs/api-reference/auth/create-access-token",
                    },
                ]
            ),
            _h("Base concepts"),
            _list(
                [
                    "A sandbox is a running isolated workload created from a Docker image, template alias, or snapshot image.",
                    "A template is a reusable image definition that can be registered directly or built from a Dockerfile.",
                    "The SDK uses the generic API first, then uses the envd data plane when a sandbox exposes it.",
                    "Compatibility routes keep E2B-shaped and Daytona-shaped clients working without replacing the generic API.",
                ]
            ),
            _code(
                "Minimal SDK flow",
                """
from my_sdk import Sandbox

sandbox = Sandbox.create(
    api_url="http://localhost:8000",
    api_key="sndbx_...",
    template_id="python:3.11",
)

result = sandbox.commands.run("python --version")
print(result.stdout)

sandbox.kill()
                """,
                "python",
            ),
        ],
        ["public", "guides"],
    ),
    _page(
        "documentation",
        "quickstart",
        "Quickstart",
        "Create a sandbox, run code, move a file, and clean up with the local Python SDK.",
        "Start",
        [
            _h("Configure the client"),
            _p("The SDK reads `MY_SDK_API_URL` and `MY_SDK_API_KEY`, or you can pass `api_url` and `api_key` directly on each call."),
            _code(
                "Environment setup",
                """
export MY_SDK_API_URL=http://localhost:8000
export MY_SDK_API_KEY=sndbx_...
                """,
            ),
            _h("Run a command"),
            _code(
                "Create and execute",
                """
from my_sdk import Sandbox

sandbox = Sandbox.create(template_id="python:3.11", timeout=3600)
try:
    sandbox.files.write("/tmp/app.py", "print('hello from sndbx')")
    result = sandbox.commands.run("python /tmp/app.py")
    print(result.exit_code, result.stdout)
finally:
    sandbox.kill()
                """,
                "python",
            ),
            _h("Use HTTP directly"),
            _code(
                "Create sandbox with curl",
                """
curl -sS http://localhost:8000/sandboxes \\
  -H "X-API-Key: sndbx_..." \\
  -H "Content-Type: application/json" \\
  -d '{"template_id":"python:3.11","timeout":3600}'
                """,
            ),
        ],
        ["sdk", "api"],
    ),
    _page(
        "documentation",
        "authentication",
        "Authentication",
        "Client APIs accept API keys directly and can exchange valid credentials for short-lived JWT access tokens.",
        "Start",
        [
            _h("Primary API key header"),
            _p("Use `X-API-Key` for generic API, E2B compatibility, Daytona compatibility, and SDK calls. The SDK sends this header automatically when `api_key` is configured."),
            _code(
                "API key request",
                """
curl http://localhost:8000/auth/me \\
  -H "X-API-Key: sndbx_..."
                """,
            ),
            _h("Bearer tokens"),
            _p("`Authorization: Bearer` accepts a short-lived JWT issued by `/auth/token`. Bearer API keys are also accepted when `AUTH_BEARER_API_KEYS_ENABLED` is enabled."),
            _code(
                "Exchange key for JWT",
                """
curl -sS http://localhost:8000/auth/token \\
  -H "X-API-Key: sndbx_..." \\
  -H "Content-Type: application/json" \\
  -d '{"ttl_seconds":3600}'
                """,
            ),
            _h("Admin routes"),
            _p("Admin observability endpoints use `X-Admin-API-Key` or `Authorization: Bearer <admin-key>`. They are documented separately from client sandbox APIs."),
        ],
        ["security"],
    ),
    _page(
        "documentation",
        "sandboxes",
        "Sandboxes",
        "Sandboxes are created from a template/image, tracked in the database, and placed on the configured execution backend.",
        "Core workflows",
        [
            _h("Create"),
            _p("Create sandboxes with `template_id`, optional metadata, resource limits, timeout, and warm-pool preference. Template aliases are resolved for the calling client before raw image fallback."),
            _code(
                "Create with resource limits",
                """
sandbox = Sandbox.create(
    template_id="python:3.11",
    cpu_limit="2",
    memory_limit="1g",
    timeout=7200,
    metadata={"purpose": "batch-job"},
)
                """,
                "python",
            ),
            _h("Lifecycle"),
            _list(
                [
                    "`sandbox.info()` returns the stored sandbox row.",
                    "`sandbox.lifecycle()` combines DB state with a runtime probe.",
                    "`sandbox.set_timeout(seconds)` refreshes the lease.",
                    "`sandbox.pause()`, `sandbox.resume()`, and `sandbox.kill()` control lifecycle.",
                ]
            ),
            _h("Snapshots"),
            _p("Filesystem snapshots are Docker commit-backed captures. Creating from a snapshot image skips warm-pool selection and starts directly from the snapshot image reference."),
        ],
        ["runtime"],
    ),
    _page(
        "documentation",
        "templates",
        "Templates",
        "Templates register reusable build inputs and can be created from base images, builder chains, or Dockerfiles.",
        "Core workflows",
        [
            _h("Builder flow"),
            _p("The Python `Template` builder composes a Dockerfile-style payload and registers it through `/templates` or `/templates/from-dockerfile`."),
            _code(
                "Build a Python template",
                """
from my_sdk import Template, wait_for_port

template = (
    Template()
    .from_python_image("3.11")
    .pip_install(["fastapi", "uvicorn"])
    .set_start_cmd("uvicorn app:app --host 0.0.0.0 --port 8000", readiness=wait_for_port(8000))
)

build = Template.build(template, name="fastapi-dev", api_key="sndbx_...")
print(build.template_id)
                """,
                "python",
            ),
            _h("Streaming builds"),
            _p("Use `Template.build_stream` with a logger callback when you want status and log events while the template build is running."),
        ],
        ["templates"],
    ),
    _page(
        "documentation",
        "commands",
        "Commands",
        "Commands run inside a sandbox either through the control plane REST route or through envd data plane when available.",
        "Core workflows",
        [
            _h("Synchronous execution"),
            _code(
                "Run command",
                """
result = sandbox.commands.run(
    "pytest -q",
    cwd="/workspace",
    env={"PYTHONUNBUFFERED": "1"},
    timeout=120,
)
print(result.exit_code, result.stdout, result.stderr)
                """,
                "python",
            ),
            _h("Streaming execution"),
            _p("`run_stream` yields JSON events with stdout chunks, stderr chunks, error events, and a final exit event."),
            _code(
                "Stream output",
                """
for event in sandbox.commands.run_stream("for i in 1 2 3; do echo $i; sleep 1; done"):
    if event["type"] == "stdout":
        print(event["chunk"], end="")
                """,
                "python",
            ),
        ],
        ["commands"],
    ),
    _page(
        "documentation",
        "filesystem",
        "Filesystem",
        "The filesystem API supports text and binary reads/writes, directory traversal, upload/download helpers, search, replace, and optional envd-backed guest access.",
        "Core workflows",
        [
            _h("Read and write"),
            _code(
                "Write and read a file",
                """
sandbox.files.write("/tmp/hello.txt", "hello")
print(sandbox.files.read("/tmp/hello.txt"))
                """,
                "python",
            ),
            _h("Guest envd mode"),
            _p("Set `MY_SANDBOX_USE_ENVD_FILESYSTEM=1` to prefer the envd data plane for filesystem operations when a sandbox has an envd connection."),
            _h("Search and replace"),
            _p("The SDK exposes higher level helpers over the generic toolbox routes: `search`, `find`, `replace`, `move`, `set_permissions`, `bulk_upload`, and `bulk_download`."),
        ],
        ["files"],
    ),
    _page(
        "documentation",
        "networking",
        "Networking and Preview URLs",
        "Sandbox ports can be addressed through ingress hostnames, public/private traffic controls, and signed preview URLs.",
        "Core workflows",
        [
            _h("Ingress hostnames"),
            _p("`sandbox.get_host(port)` returns a host shaped like `{port}-{sandbox_id}.{sandbox_domain}`. Private sandboxes require the traffic access token at the ingress layer."),
            _code(
                "Get a public host",
                """
host = sandbox.get_host(8000)
print(f"https://{host}")
                """,
                "python",
            ),
            _h("Preview URLs"),
            _list(
                [
                    "`preview_url(port)` returns a direct preview URL for a port.",
                    "`signed_preview_url(port, expires_in_seconds=...)` returns a tokenized URL.",
                    "`set_public(True)` allows traffic without the private ingress token.",
                ]
            ),
        ],
        ["network"],
    ),
    _page(
        "documentation",
        "snapshots",
        "Snapshots",
        "Snapshots capture the writable filesystem layer of a running sandbox and can be used as the image source for later sandboxes.",
        "Core workflows",
        [
            _h("Create a snapshot"),
            _code(
                "Snapshot and reuse",
                """
snapshot = sandbox.create_snapshot(label="after-install")
new_sandbox = Sandbox.create(from_snapshot_image=snapshot.image_ref)
                """,
                "python",
            ),
            _h("Limitations"),
            _p("Snapshot creation depends on Docker commit support in the execution backend. If commit is unavailable, the API returns a clear 501-style error."),
        ],
        ["snapshot"],
    ),
    _page(
        "documentation",
        "agents",
        "Agents",
        "Agent endpoints manage named Python agent tasks associated with a sandbox and expose message history APIs.",
        "Core workflows",
        [
            _h("Spawn and message"),
            _code(
                "Agent lifecycle",
                """
agent = sandbox.spawn_agent(
    agent_name="build_loop_demo",
    config={"single_run": True},
)
sandbox.send_agent_message(agent["agent_id"], "start")
messages = sandbox.get_agent_messages(agent["agent_id"])
                """,
                "python",
            ),
            _h("Transport"),
            _p("For real-time agent traffic, the SDK also exposes helper functions for opening guest and agent WebSockets."),
        ],
        ["agents"],
    ),
    _page(
        "documentation",
        "observability",
        "Observability",
        "Operator observability combines gateway load, warm-pool health, image status, events, and sandbox timelines.",
        "Operations",
        [
            _h("Portal view"),
            _p("The existing portal observability pages show runtime-gateway pods, CPU/load sparklines, Docker graph storage, warm sandboxes, running sandboxes, image repair state, and events."),
            _h("Admin API"),
            _p("The admin observability API is protected by admin credentials and documented under API Reference. Use it for scripts and debugging, not public SDK workflows."),
        ],
        ["admin"],
    ),
    _page(
        "sdk-reference",
        "overview",
        "Python SDK Overview",
        "`my_sandbox_sdk` provides synchronous and asynchronous clients for the generic API plus E2B-shaped template and WebSocket helpers.",
        "Start",
        [
            _h("Exports"),
            _table(
                ["Area", "Sync", "Async"],
                [
                    ["Sandbox", "Sandbox", "AsyncSandbox"],
                    ["Commands", "Commands", "AsyncCommands"],
                    ["Filesystem", "Filesystem", "AsyncFilesystem"],
                    ["Process", "Process", "AsyncProcess"],
                    ["PTY", "Pty", "AsyncPty"],
                    ["Git", "Git", "AsyncGit"],
                    ["Templates", "Template", "AsyncTemplate"],
                ],
            ),
            _h("Configuration"),
            _p("The SDK defaults to `MY_SDK_API_URL`, `MY_SDK_API_KEY`, and `MY_SDK_REQUEST_TIMEOUT`. The default request timeout is intentionally long because sandbox creation can block on image pulls."),
        ],
        ["python"],
    ),
    _page(
        "sdk-reference",
        "errors",
        "Errors",
        "SDK exceptions normalize API failures into typed Python exceptions.",
        "Core",
        [
            _methods(
                [
                    {"name": "SandboxException", "signature": "SandboxException(message)", "text": "Base class for SDK failures."},
                    {"name": "AuthenticationException", "signature": "AuthenticationException(message)", "text": "Raised for HTTP 401 authentication failures."},
                    {"name": "SandboxNotFoundException", "signature": "SandboxNotFoundException(message)", "text": "Raised for missing sandboxes or unmatched API routes."},
                    {"name": "CommandException", "signature": "CommandException(message, exit_code=None)", "text": "Raised for failed command/data-plane execution."},
                    {"name": "FileNotFoundException", "signature": "FileNotFoundException(path)", "text": "Raised when a requested file is missing."},
                    {"name": "TimeoutException", "signature": "TimeoutException(message)", "text": "Raised on request or operation timeouts."},
                    {"name": "APIException", "signature": "APIException(status_code, message)", "text": "Raised for non-401/404 API errors."},
                ]
            ),
            _h("Authentication failure"),
            _p("A 401 response becomes `AuthenticationException(\"Authentication failed. Check your API key.\")`. In practice, verify the key value, the selected base URL, and whether the key belongs to the active portal client."),
        ],
        ["exceptions"],
    ),
    _page(
        "sdk-reference",
        "sandbox",
        "Sandbox",
        "`Sandbox` owns lifecycle, connection, networking, metrics, snapshots, and module access for one running sandbox.",
        "Core",
        [
            _methods(
                [
                    {"name": "create", "signature": "Sandbox.create(template_id='python:3.11', metadata=None, timeout=3600, cpu_limit='1', memory_limit='512m', warmpool_size=None, from_snapshot_image=None, api_url=None, api_key=None)", "text": "Create a sandbox and seed connection metadata returned by the API."},
                    {"name": "list", "signature": "Sandbox.list(api_url=None, api_key=None)", "text": "List sandboxes visible to the credential."},
                    {"name": "attach", "signature": "Sandbox.attach(sandbox_id, api_url=None, api_key=None)", "text": "Attach a client object to an existing sandbox."},
                    {"name": "connect", "signature": "Sandbox.connect(sandbox_id, port=8765, api_url=None, api_key=None)", "text": "Attach and immediately fetch E2B-style connection metadata."},
                    {"name": "health / ready / diagnostics", "signature": "Sandbox.health(), Sandbox.ready(), Sandbox.diagnostics()", "text": "Call service health endpoints."},
                    {"name": "lifecycle", "signature": "sandbox.lifecycle()", "text": "Return state, running probe, timeout, and lease expiry."},
                    {"name": "kill / pause / resume", "signature": "sandbox.kill(), sandbox.pause(), sandbox.resume()", "text": "Control sandbox lifecycle."},
                    {"name": "metrics", "signature": "sandbox.metrics()", "text": "Return CPU, memory, and runtime metric details."},
                ]
            ),
            _h("Modules"),
            _p("Use `sandbox.commands`, `sandbox.files`, `sandbox.process`, `sandbox.pty`, and `sandbox.git` for scoped operations against the same sandbox."),
        ],
        ["lifecycle"],
    ),
    _page(
        "sdk-reference",
        "commands",
        "Commands",
        "`Commands` executes shell commands in a sandbox and can stream output events.",
        "Modules",
        [
            _methods(
                [
                    {"name": "run", "signature": "sandbox.commands.run(command, cwd=None, env=None, envs=None, timeout=None, user=None)", "text": "Run a command and return `CommandResult`."},
                    {"name": "run_stream", "signature": "sandbox.commands.run_stream(command, cwd=None, env=None, timeout=None, user=None)", "text": "Yield event dictionaries for stdout, stderr, error, and final exit."},
                    {"name": "run_python", "signature": "sandbox.commands.run_python(code, cwd=None, timeout=None)", "text": "Run Python code through the command layer."},
                    {"name": "list", "signature": "sandbox.commands.list()", "text": "List processes when supported by the backend."},
                    {"name": "kill", "signature": "sandbox.commands.kill(pid)", "text": "Terminate a process by PID."},
                ]
            ),
            _code(
                "Streaming example",
                """
for event in sandbox.commands.run_stream("python -u train.py"):
    if event.get("type") == "stdout":
        print(event.get("chunk", ""), end="")
                """,
                "python",
            ),
        ],
        ["commands"],
    ),
    _page(
        "sdk-reference",
        "filesystem",
        "Filesystem",
        "`Filesystem` wraps generic file routes and optional envd guest filesystem routes.",
        "Modules",
        [
            _methods(
                [
                    {"name": "list", "signature": "sandbox.files.list(path='/')", "text": "Return `FilesystemEntry` rows for a directory."},
                    {"name": "read / read_bytes", "signature": "sandbox.files.read(path), sandbox.files.read_bytes(path)", "text": "Read text or raw bytes."},
                    {"name": "write / write_bytes", "signature": "sandbox.files.write(path, content), sandbox.files.write_bytes(path, content)", "text": "Write text or raw bytes."},
                    {"name": "upload / download", "signature": "sandbox.files.upload(local_path, sandbox_path), sandbox.files.download(sandbox_path, local_path)", "text": "Move files between host and sandbox."},
                    {"name": "bulk_upload / bulk_download", "signature": "sandbox.files.bulk_upload(files), sandbox.files.bulk_download(paths)", "text": "Move multiple files in one request."},
                    {"name": "search / find / replace", "signature": "sandbox.files.search(path, pattern), sandbox.files.find(path, pattern), sandbox.files.replace(files, pattern, new_value)", "text": "Search paths or text and optionally replace matches."},
                    {"name": "exists", "signature": "sandbox.files.exists(path)", "text": "Return true when metadata lookup succeeds."},
                ]
            ),
        ],
        ["files"],
    ),
    _page(
        "sdk-reference",
        "template",
        "Template",
        "`Template` is an E2B-shaped builder for reusable sandbox images and warm snapshots.",
        "Modules",
        [
            _methods(
                [
                    {"name": "from_python_image", "signature": "Template().from_python_image(version='3.11')", "text": "Start from an official Python image."},
                    {"name": "from_docker_image", "signature": "Template().from_docker_image(image)", "text": "Start from an arbitrary Docker image."},
                    {"name": "run_cmd", "signature": "template.run_cmd(cmd)", "text": "Add a shell command to the build flow."},
                    {"name": "pip_install / apt_install / npm_install", "signature": "template.pip_install([...])", "text": "Convenience package-install helpers."},
                    {"name": "copy", "signature": "template.copy(host_path, container_path)", "text": "Add files into the build context."},
                    {"name": "set_start_cmd", "signature": "template.set_start_cmd(cmd, readiness=None)", "text": "Set runtime start command and optional readiness marker."},
                    {"name": "build", "signature": "Template.build(template, name=None, api_url=None, api_key=None)", "text": "Register/build and return `BuildInfo`."},
                    {"name": "build_stream", "signature": "Template.build_stream(template, name=None, on_log=None, api_url=None, api_key=None)", "text": "Build while streaming server log events."},
                    {"name": "list_registered", "signature": "Template.list_registered(api_url=None, api_key=None)", "text": "List templates visible to the credential."},
                ]
            ),
            _h("Readiness helpers"),
            _p("`wait_for_timeout(ms)` and `wait_for_port(port, timeout_ms=60000)` turn common readiness waits into template build markers."),
        ],
        ["templates"],
    ),
    _page(
        "sdk-reference",
        "logger",
        "Template Logger",
        "Template build streaming accepts a callback that receives structured build log entries.",
        "Modules",
        [
            _h("Default logger"),
            _p("`default_build_logger()` returns a simple callback that prints build log entries in a readable format."),
            _code(
                "Use build logger",
                """
from my_sdk import Template, default_build_logger

template = Template().from_python_image("3.11").pip_install(["requests"])
build = Template.build_stream(
    template,
    name="requests-runner",
    api_key="sndbx_...",
    on_log=default_build_logger(),
)
                """,
                "python",
            ),
            _h("Custom logger"),
            _p("A custom logger can persist progress events, render a UI, or trigger alerts. Expect dictionary-like log entries from streamed build output."),
        ],
        ["templates"],
    ),
    _page(
        "sdk-reference",
        "process",
        "Process",
        "`Process` exposes Daytona-toolbox-shaped process execution and session APIs through generic sandbox routes.",
        "Modules",
        [
            _methods(
                [
                    {"name": "execute", "signature": "sandbox.process.execute(command, cwd=None, env=None, timeout=None)", "text": "Execute a process through the toolbox API."},
                    {"name": "code_run", "signature": "sandbox.process.code_run(code, language='python')", "text": "Run source code in an interpreter-style flow."},
                    {"name": "create_session", "signature": "sandbox.process.create_session(command=None, cwd=None, env=None)", "text": "Create a persistent process session."},
                    {"name": "execute_session_command", "signature": "sandbox.process.execute_session_command(session_id, command)", "text": "Run a command inside a process session."},
                    {"name": "entrypoint_logs", "signature": "sandbox.process.entrypoint_logs()", "text": "Return entrypoint logs when the backend exposes them."},
                ]
            ),
        ],
        ["toolbox"],
    ),
    _page(
        "sdk-reference",
        "pty",
        "PTY",
        "`Pty` manages interactive terminal sessions and WebSocket connection URLs.",
        "Modules",
        [
            _methods(
                [
                    {"name": "create_session", "signature": "sandbox.pty.create_session(command=None, cwd=None, rows=24, cols=80)", "text": "Create an interactive PTY session."},
                    {"name": "list_sessions", "signature": "sandbox.pty.list_sessions()", "text": "List active PTY sessions."},
                    {"name": "resize_session", "signature": "sandbox.pty.resize_session(session_id, rows, cols)", "text": "Resize a terminal session."},
                    {"name": "connect_url", "signature": "sandbox.pty.connect_url(session_id, **params)", "text": "Build the WebSocket URL for client-side PTY attach."},
                ]
            ),
        ],
        ["toolbox"],
    ),
    _page(
        "sdk-reference",
        "git",
        "Git",
        "`Git` wraps common repository operations inside a sandbox workspace.",
        "Modules",
        [
            _methods(
                [
                    {"name": "init / clone", "signature": "sandbox.git.init(path), sandbox.git.clone(url, path)", "text": "Create or clone a repository."},
                    {"name": "status / branches / history", "signature": "sandbox.git.status(path)", "text": "Inspect repository state."},
                    {"name": "add / commit", "signature": "sandbox.git.add(path, files=None), sandbox.git.commit(path, message, author_name=None, author_email=None)", "text": "Stage and commit changes."},
                    {"name": "pull / push", "signature": "sandbox.git.pull(path), sandbox.git.push(path)", "text": "Synchronize with remotes."},
                    {"name": "configure_user", "signature": "sandbox.git.configure_user(name, email, scope='global', path=None)", "text": "Set Git identity."},
                ]
            ),
        ],
        ["toolbox"],
    ),
    _page(
        "sdk-reference",
        "models",
        "Models",
        "SDK model classes convert API dictionaries into typed Python objects for common responses.",
        "Modules",
        [
            _table(
                ["Model", "Purpose"],
                [
                    ["SandboxInfo", "Sandbox id, state, timestamps, metadata, runtime, domain, and tokens."],
                    ["SandboxLifecycle", "State, running probe, timeout, and lease expiry."],
                    ["CommandResult", "Exit code, stdout, stderr, PID, and execution time."],
                    ["FilesystemEntry", "Path, name, type, size, permissions, and modified time."],
                    ["BuildInfo", "Template build result, template id, build id, status, and logs/status URLs."],
                    ["TemplateDefinition", "Registered template details."],
                    ["SandboxMetrics", "Runtime metrics for CPU, memory, and related values."],
                ],
            ),
        ],
        ["models"],
    ),
    _page(
        "api-reference",
        "overview",
        "API Reference Overview",
        "The API reference covers the generic SDK-facing API first, then compatibility APIs and admin-only observability endpoints.",
        "Start",
        [
            _h("Conventions"),
            _list(
                [
                    "Client routes accept `X-API-Key` or `Authorization: Bearer`.",
                    "Admin observability routes accept `X-Admin-API-Key` or an admin bearer value.",
                    "Responses are JSON unless the endpoint is an SSE stream, raw file download, or WebSocket route.",
                    "Internal runtime routes are intentionally excluded from public reference pages.",
                ]
            ),
            _h("Base URLs"),
            _table(
                ["Surface", "Base"],
                [
                    ["Generic API", "http://localhost:8000"],
                    ["Portal", "/portal"],
                    ["Docs", "/docs"],
                    ["OpenAPI schema", "/openapi.json"],
                ],
            ),
        ],
        ["http"],
    ),
    _page(
        "api-reference",
        "auth",
        "Auth API",
        "Exchange credentials for short-lived access tokens and inspect the authenticated principal.",
        "Core",
        [
            _endpoints(
                "Auth endpoints",
                [
                    _endpoint("POST", "/auth/token", "Exchange a valid API key or JWT for a short-lived JWT.", request='{"ttl_seconds":3600}', response="access_token, token_type, expires_in, client_id, key_id, auth_type"),
                    _endpoint("GET", "/auth/me", "Return the authenticated client/key principal.", response="client_id, key_id, key_name, key_prefix, email, display_name, auth_type"),
                    _endpoint("GET", "/health", "Cheap Kubernetes-safe liveness check.", auth="none", response="status, version, api_service_role"),
                    _endpoint("GET", "/ready", "Cheap readiness check.", auth="none", response="status, version, api_service_role"),
                    _endpoint("GET", "/diagnostics/health", "Human diagnostic health with runtime and warm-pool context.", auth="none", response="runtime status, execution plane, warm-pool and gateway diagnostics"),
                ],
            )
        ],
        ["auth"],
    ),
    _page(
        "api-reference",
        "sandboxes",
        "Sandboxes API",
        "Create, inspect, list, pause, resume, kill, refresh, snapshot, and measure sandboxes.",
        "Core",
        [
            _endpoints(
                "Sandbox endpoints",
                [
                    _endpoint("POST", "/sandboxes", "Create a sandbox.", request="template_id, metadata, env_vars, cpu_limit, memory_limit, timeout, warmpool_size, from_snapshot_image", response="SandboxResponse", example='curl -sS http://localhost:8000/sandboxes -H "X-API-Key: sndbx_..." -H "Content-Type: application/json" -d \'{"template_id":"python:3.11"}\''),
                    _endpoint("GET", "/sandboxes", "List sandboxes visible to the credential.", response="list[SandboxResponse]"),
                    _endpoint("GET", "/sandboxes/{sandbox_id}", "Get one sandbox.", response="SandboxResponse"),
                    _endpoint("GET", "/sandboxes/{sandbox_id}/status", "Get lifecycle state and runtime liveness.", response="SandboxLifecycleResponse"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/timeout", "Refresh sandbox lease timeout.", request="timeout_seconds or timeout", response="SandboxTimeoutRefreshResponse"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/warm-pool/size", "Resize the sandbox's template/cpu/memory warm-pool segment.", request="warmpool_size", response="WarmPoolResizeResponse"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/pause", "Pause sandbox workload.", response="ok/state payload"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/resume", "Resume paused sandbox workload.", response="ok/state payload"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/kill", "Terminate and mark sandbox killed.", response="ok payload"),
                    _endpoint("GET", "/sandboxes/{sandbox_id}/metrics", "Return sandbox metrics.", response="runtime metric object"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/snapshot", "Create filesystem snapshot.", request="label", response="SnapshotRecordResponse"),
                    _endpoint("GET", "/sandboxes/{sandbox_id}/snapshots", "List snapshots created from a sandbox.", response="list[SnapshotRecordResponse]"),
                ],
            )
        ],
        ["runtime"],
    ),
    _page(
        "api-reference",
        "commands",
        "Commands API",
        "Run commands in a sandbox and inspect active process rows when supported by the runtime.",
        "Core",
        [
            _endpoints(
                "Command endpoints",
                [
                    _endpoint("POST", "/sandboxes/{sandbox_id}/commands/run", "Run a command and wait for completion.", request="command, cwd, env, timeout, user", response="CommandResponse"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/commands/run/stream", "Run a command and stream SSE JSON events.", request="command, cwd, env, timeout, user", response="text/event-stream events: stdout, stderr, error, exit"),
                    _endpoint("GET", "/sandboxes/{sandbox_id}/commands", "List command/process records.", response="list[ProcessInfo]"),
                ],
            )
        ],
        ["commands"],
    ),
    _page(
        "api-reference",
        "filesystem",
        "Filesystem API",
        "Read, write, search, upload, download, and mutate sandbox filesystem content.",
        "Core",
        [
            _endpoints(
                "Basic file endpoints",
                [
                    _endpoint("GET", "/sandboxes/{sandbox_id}/files", "List directory entries.", request="query: path", response="ListFilesResponse"),
                    _endpoint("GET", "/sandboxes/{sandbox_id}/files/read", "Read a text file.", request="query: path", response="content, encoding"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/files/write", "Write a text file.", request="path, content, encoding", response="WriteFileResponse"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/files/delete", "Delete a file or directory.", request="path, recursive", response="ok payload"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/files/mkdir", "Create a directory.", request="path, mode", response="ok payload"),
                ],
            ),
            _endpoints(
                "Extended file endpoints",
                [
                    _endpoint("GET", "/sandboxes/{sandbox_id}/files/info", "Return metadata for one path.", request="query: path", response="metadata object"),
                    _endpoint("GET", "/sandboxes/{sandbox_id}/files/download", "Download raw file bytes.", request="query: path", response="binary file response"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/files/upload", "Upload raw/multipart file content.", request="query/body file payload", response="bytes written payload"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/files/bulk-download", "Download several files as an archive payload.", request="paths", response="binary/archive payload"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/files/bulk-upload", "Upload several files.", request="files map", response="ok payload"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/files/move", "Move or rename a path.", request="source, destination", response="ok payload"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/files/permissions", "Set permissions.", request="path, mode", response="ok payload"),
                    _endpoint("GET", "/sandboxes/{sandbox_id}/files/search", "Find paths by pattern.", request="query: path, pattern", response="matches"),
                    _endpoint("GET", "/sandboxes/{sandbox_id}/files/find", "Find text matches in files.", request="query: path, pattern", response="matches"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/files/replace", "Replace text in files.", request="files, pattern, new_value", response="replace results"),
                ],
            ),
        ],
        ["files"],
    ),
    _page(
        "api-reference",
        "templates",
        "Templates API",
        "Register logical templates and build Dockerfile-backed templates for later sandbox creation.",
        "Core",
        [
            _endpoints(
                "Template endpoints",
                [
                    _endpoint("POST", "/templates", "Register a logical template from base image/start command fields.", request="template_id, base_image, env, start_cmd, settle_seconds, ready_cmd, warm_snapshot_image", response="TemplateDefinitionResponse"),
                    _endpoint("POST", "/templates/from-dockerfile", "Register/build a template from Dockerfile content.", request="template_id, dockerfile, image_tag, build_args, context_tar_gzip_base64, env, start_cmd, ready_cmd, settle_seconds", response="TemplateDefinitionResponse"),
                    _endpoint("POST", "/templates/from-dockerfile/stream", "Stream template build status/log events.", request="same as from-dockerfile", response="text/event-stream"),
                    _endpoint("GET", "/templates", "List visible registered templates.", response="list[TemplateDefinitionResponse]"),
                    _endpoint("GET", "/templates/{template_id}", "Get one registered template by id/alias.", response="TemplateDefinitionResponse"),
                ],
            )
        ],
        ["templates"],
    ),
    _page(
        "api-reference",
        "toolbox",
        "Toolbox API",
        "Generic process, PTY, Git, port, and system-metric routes used by the local SDK and Daytona compatibility layer.",
        "Core",
        [
            _endpoints(
                "Process endpoints",
                [
                    _endpoint("POST", "/sandboxes/{sandbox_id}/process/execute", "Execute a toolbox command.", request="command/cmd, cwd, env, timeout", response="command result object"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/process/code-run", "Run source code in a selected language.", request="code, language", response="result object"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/process/sessions", "Create process session.", response="session object"),
                    _endpoint("GET", "/sandboxes/{sandbox_id}/process/sessions", "List process sessions.", response="sessions object"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/process/sessions/{session_id}/commands", "Execute command inside session.", response="command object"),
                    _endpoint("GET", "/sandboxes/{sandbox_id}/process/entrypoint/logs", "Read entrypoint logs.", response="logs object or WebSocket on WS route"),
                ],
            ),
            _endpoints(
                "PTY and Git endpoints",
                [
                    _endpoint("POST", "/sandboxes/{sandbox_id}/pty/sessions", "Create PTY session.", response="session object"),
                    _endpoint("GET", "/sandboxes/{sandbox_id}/pty/sessions", "List PTY sessions.", response="sessions object"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/pty/sessions/{session_id}/resize", "Resize PTY.", request="rows, cols", response="session object"),
                    _endpoint("WS", "/sandboxes/{sandbox_id}/pty/sessions/{session_id}/connect", "Attach to PTY over WebSocket.", response="WebSocket stream"),
                    _endpoint("GET/POST", "/sandboxes/{sandbox_id}/git/status, /git/init, /git/clone, /git/add, /git/commit, /git/pull, /git/push, ...", "Git operation endpoints grouped under `/git/*`.", response="operation-specific JSON"),
                    _endpoint("GET", "/sandboxes/{sandbox_id}/ports/{port}/in-use", "Check whether a port is listening.", response="in_use payload"),
                    _endpoint("GET", "/sandboxes/{sandbox_id}/system/metrics", "Return system metrics.", response="metrics payload"),
                ],
            ),
        ],
        ["toolbox"],
    ),
    _page(
        "api-reference",
        "agents",
        "Agents API",
        "Spawn, inspect, stop, and message sandbox-associated agents.",
        "Core",
        [
            _endpoints(
                "Agent endpoints",
                [
                    _endpoint("POST", "/sandboxes/{sandbox_id}/agents/spawn", "Spawn an agent.", request="agent_name, agent_code, config, auto_start", response="AgentResponse"),
                    _endpoint("GET", "/sandboxes/{sandbox_id}/agents", "List sandbox agents.", response="list payload"),
                    _endpoint("GET", "/sandboxes/{sandbox_id}/agents/{agent_id}", "Get one agent.", response="AgentResponse"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/agents/{agent_id}/kill", "Stop an agent.", request="force", response="ok payload"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/agents/{agent_id}/messages", "Append/send an agent message.", request="role/content payload", response="AgentMessageResponse"),
                    _endpoint("GET", "/sandboxes/{sandbox_id}/agents/{agent_id}/messages", "List agent messages.", request="query: limit", response="messages payload"),
                ],
            )
        ],
        ["agents"],
    ),
    _page(
        "api-reference",
        "connections",
        "Connections API",
        "Fetch data-plane URLs and ingress credentials for sandbox guest ports and envd.",
        "Core",
        [
            _endpoints(
                "Connection endpoints",
                [
                    _endpoint("GET", "/sandboxes/{sandbox_id}/connection", "Return URL/token for an arbitrary guest port.", request="query: port, scheme", response="SandboxGuestConnectionResponse"),
                    _endpoint("GET", "/sandboxes/{sandbox_id}/e2b-connection", "Return E2B-shaped WebSocket connection info.", request="query: port", response="SandboxE2bConnectionResponse"),
                    _endpoint("GET", "/sandboxes/{sandbox_id}/envd-connection", "Return envd HTTP base URL and access token.", response="SandboxEnvdConnectionResponse"),
                    _endpoint("PUT", "/sandboxes/{sandbox_id}/labels", "Set sandbox labels.", request="labels object", response="labels payload"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/network-settings", "Update network settings.", request="settings object", response="settings payload"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/public/{is_public}", "Toggle public traffic.", response="public traffic payload"),
                    _endpoint("GET", "/sandboxes/{sandbox_id}/ports/{port}/preview-url", "Return preview URL.", response="url payload"),
                    _endpoint("GET", "/sandboxes/{sandbox_id}/ports/{port}/signed-preview-url", "Return signed preview URL.", request="query: expires_in_seconds", response="url/token payload"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/ssh-access", "Create SSH access credential.", request="expires_in_minutes", response="ssh payload"),
                    _endpoint("DELETE", "/sandboxes/{sandbox_id}/ssh-access", "Revoke SSH access.", response="ok payload"),
                ],
            )
        ],
        ["network"],
    ),
    _page(
        "api-reference",
        "observability",
        "Admin Observability API",
        "Admin-only API for runtime health, gateway placement, warm pools, image repair, events, and sandbox timelines.",
        "Admin",
        [
            _endpoints(
                "Admin endpoints",
                [
                    _endpoint("GET", "/admin/observability/summary", "Cluster/runtime health summary.", auth="X-Admin-API-Key or admin bearer", response="summary payload"),
                    _endpoint("GET", "/admin/observability/gateways", "Runtime-gateway pod diagnostics and sandbox placement.", auth="X-Admin-API-Key or admin bearer", response="gateway list"),
                    _endpoint("GET", "/admin/observability/warm-pools", "Warm-pool segment details.", auth="X-Admin-API-Key or admin bearer", response="warm pool list"),
                    _endpoint("GET", "/admin/observability/templates/images", "Template image status and repair hints.", auth="X-Admin-API-Key or admin bearer", response="image list"),
                    _endpoint("GET", "/admin/observability/events", "Recent observability events.", auth="X-Admin-API-Key or admin bearer", request="query filters", response="event list"),
                    _endpoint("GET", "/admin/observability/sandboxes/{sandbox_id}/timeline", "Timeline for one sandbox.", auth="X-Admin-API-Key or admin bearer", response="timeline payload"),
                ],
            )
        ],
        ["admin"],
    ),
    _page(
        "api-reference",
        "e2b-compat",
        "E2B Compatibility API",
        "E2B-shaped routes are preserved for clients that expect E2B sandbox and template paths.",
        "Compatibility",
        [
            _endpoints(
                "Sandbox compatibility",
                [
                    _endpoint("POST", "/sandboxes", "Create sandbox using E2B-compatible body shape.", response="E2B-shaped sandbox response"),
                    _endpoint("GET", "/sandboxes/{sandbox_id}", "Get sandbox.", response="E2B-shaped sandbox response"),
                    _endpoint("DELETE", "/sandboxes/{sandbox_id}", "Delete sandbox.", response="ok payload"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/snapshots", "Create snapshot.", response="snapshot payload"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/timeout", "Set timeout.", response="timeout payload"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/pause", "Pause sandbox.", response="ok payload"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/resume", "Resume sandbox.", response="ok payload"),
                    _endpoint("POST", "/sandboxes/{sandbox_id}/connect", "Return connection info.", response="connection payload"),
                    _endpoint("PUT", "/sandboxes/{sandbox_id}/network", "Update network config.", response="network payload"),
                    _endpoint("GET", "/v2/sandboxes", "List sandboxes in v2-compatible shape.", response="list payload"),
                    _endpoint("GET", "/snapshots", "List snapshots.", response="snapshot list"),
                ],
            ),
            _endpoints(
                "Template compatibility",
                [
                    _endpoint("POST", "/v3/templates", "Create template in v3-compatible shape.", response="template payload"),
                    _endpoint("POST", "/v2/templates/{template_id}/builds/{build_id}", "Start or update template build.", response="build payload"),
                    _endpoint("GET", "/templates/{template_id}/builds/{build_id}/status", "Get build status.", response="build status"),
                    _endpoint("GET", "/templates/aliases/{alias}", "Resolve alias.", response="template payload"),
                    _endpoint("POST", "/templates/tags", "Add template tag.", response="tag payload"),
                    _endpoint("DELETE", "/templates/tags", "Delete template tag.", response="ok payload"),
                    _endpoint("GET", "/templates/{template_id}/tags", "List tags.", response="tag list"),
                    _endpoint("GET/PUT", "/templates/{template_id}/files/{hash_}", "Fetch or upload template build file blobs.", response="file payload"),
                    _endpoint("DELETE", "/templates/{template_id}", "Delete template.", response="ok payload"),
                ],
            ),
        ],
        ["compat"],
    ),
    _page(
        "api-reference",
        "daytona-compat",
        "Daytona Compatibility API",
        "Daytona-shaped routes cover sandbox lifecycle, snapshots, secrets/volumes placeholders, toolbox APIs, process, PTY, and Git.",
        "Compatibility",
        [
            _endpoints(
                "Sandbox compatibility",
                [
                    _endpoint("GET", "/health/ready", "Daytona-style readiness.", response="ready payload"),
                    _endpoint("GET", "/config", "Compatibility config payload.", response="config payload"),
                    _endpoint("POST", "/sandbox", "Create sandbox.", response="Daytona sandbox payload"),
                    _endpoint("GET", "/sandbox", "List sandboxes.", response="list payload"),
                    _endpoint("GET", "/sandbox/paginated", "Paginated sandbox list.", response="paginated payload"),
                    _endpoint("GET", "/sandbox/{sandbox_id_or_name}", "Get sandbox by id/name.", response="sandbox payload"),
                    _endpoint("DELETE", "/sandbox/{sandbox_id_or_name}", "Delete sandbox.", response="ok payload"),
                    _endpoint("POST", "/sandbox/{sandbox_id_or_name}/start|stop|pause", "Lifecycle actions.", response="state payload"),
                    _endpoint("POST", "/sandbox/{sandbox_id_or_name}/snapshot", "Create snapshot.", response="snapshot payload"),
                    _endpoint("GET", "/sandbox/{sandbox_id}/telemetry/metrics", "Sandbox telemetry metrics.", response="metrics payload"),
                ],
            ),
            _endpoints(
                "Toolbox compatibility",
                [
                    _endpoint("GET/POST", "/{sandbox_id}/toolbox/files/* and /toolbox/{sandbox_id}/toolbox/files/*", "File download, upload, search, replace, metadata, permissions, and bulk operations.", response="operation-specific JSON/binary"),
                    _endpoint("GET/POST/DELETE", "/{sandbox_id}/toolbox/process/*", "Process sessions, commands, logs, input, and entrypoint logs.", response="operation-specific JSON/WebSocket"),
                    _endpoint("GET/POST/DELETE", "/{sandbox_id}/toolbox/process/pty/*", "PTY sessions and WebSocket attach.", response="operation-specific JSON/WebSocket"),
                    _endpoint("GET/POST/DELETE", "/{sandbox_id}/toolbox/git/*", "Git init, clone, status, branches, commit, remotes, config, credentials, and history.", response="operation-specific JSON"),
                    _endpoint("GET", "/{sandbox_id}/toolbox/system/metrics", "System metrics.", response="metrics payload"),
                ],
            ),
        ],
        ["compat"],
    ),
]


def _api_detail_page(endpoint: dict[str, Any]) -> dict[str, Any]:
    return _page(
        "api-reference",
        endpoint["slug"],
        endpoint["title"],
        endpoint["description"],
        endpoint["group"],
        [{"type": "api_detail", "endpoint": endpoint}],
        [endpoint["method"], "endpoint"],
    )


def _link_endpoint_summaries(endpoints: list[dict[str, Any]]) -> None:
    endpoint_index = {
        (endpoint["method"], endpoint["path"]): endpoint["href"]
        for endpoint in endpoints
    }
    for page in PAGES:
        if page.get("section") != "api-reference":
            continue
        for block in page.get("blocks", []):
            if block.get("type") != "endpoints":
                continue
            for item in block.get("items", []):
                href = endpoint_index.get((item.get("method", ""), item.get("path", "")))
                if href:
                    item["href"] = href


API_REFERENCE_SUMMARY_SLUGS = {
    "overview",
    "auth",
    "sandboxes",
    "commands",
    "filesystem",
    "templates",
    "toolbox",
    "agents",
    "connections",
    "observability",
    "e2b-compat",
    "daytona-compat",
}


def _endpoint_reference_order(endpoint: dict[str, Any]) -> tuple[int, str]:
    group = str(endpoint.get("group") or "")
    if group == "Auth":
        return (0, group)
    if group == "System":
        return (99, group)
    return (10, "")


def _ordered_api_reference_endpoints(endpoints: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        endpoint
        for _, endpoint in sorted(
            enumerate(endpoints),
            key=lambda item: (_endpoint_reference_order(item[1]), item[0]),
        )
    ]


PAGES[:] = [
    page
    for page in PAGES
    if not (
        page.get("section") == "api-reference"
        and page.get("slug") in API_REFERENCE_SUMMARY_SLUGS
    )
]


from docs_api_reference import MAIN_API_ENDPOINTS

API_REFERENCE_ENDPOINTS = _ordered_api_reference_endpoints(MAIN_API_ENDPOINTS)
PAGES.extend(_api_detail_page(endpoint) for endpoint in API_REFERENCE_ENDPOINTS)
_link_endpoint_summaries(API_REFERENCE_ENDPOINTS)


SECTION_BY_ID = {section["id"]: section for section in SECTIONS}
PAGE_BY_KEY = {(page["section"], page["slug"]): page for page in PAGES}


def get_section(section_id: str) -> dict[str, str] | None:
    return SECTION_BY_ID.get(section_id)


def get_default_slug(section_id: str) -> str | None:
    section = get_section(section_id)
    return section["default"] if section else None


def get_page(section_id: str, slug: str) -> dict[str, Any] | None:
    return PAGE_BY_KEY.get((section_id, slug))


def top_nav(active_section: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for section in SECTIONS:
        href = f"/docs/{section['id']}/{section['default']}"
        items.append({**section, "href": href, "active": section["id"] == active_section})
    return items


def sidebar_groups(active_section: str, active_slug: str) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    index: dict[str, dict[str, Any]] = {}
    for page in PAGES:
        if page["section"] != active_section:
            continue
        group_label = page["group"]
        group = index.get(group_label)
        if group is None:
            group = {"label": group_label, "pages": [], "collapsible": True, "open": False}
            index[group_label] = group
            groups.append(group)
        active = page["slug"] == active_slug
        if active:
            group["open"] = True
        group["pages"].append({**page, "active": active})
    return groups


def page_toc(page: dict[str, Any]) -> list[dict[str, str]]:
    toc: list[dict[str, str]] = []
    for block in page.get("blocks", []):
        if block.get("type") == "heading":
            toc.append({"id": block["id"], "title": block["title"]})
        elif block.get("type") == "endpoints":
            toc.append({"id": block["id"], "title": block["title"]})
        elif block.get("type") == "api_detail":
            endpoint = block.get("endpoint") or {}
            toc.append({"id": "authorization", "title": "Authorization"})
            if endpoint.get("path_params"):
                toc.append({"id": "path-parameters", "title": "Path Parameters"})
            if endpoint.get("query_params"):
                toc.append({"id": "query-parameters", "title": "Query Parameters"})
            if endpoint.get("body_params"):
                toc.append({"id": "body", "title": "Body"})
            toc.append({"id": "responses", "title": "Responses"})
    return toc


def not_found_page(section_id: str = "documentation") -> dict[str, Any]:
    section = section_id if section_id in SECTION_BY_ID else "documentation"
    return _page(
        section,
        "not-found",
        "Page Not Found",
        "The docs page you requested does not exist.",
        "Missing",
        [
            _h("Try another page"),
            _p("Use the sidebar to select an existing guide or reference page."),
            _cards(
                [
                    {"title": "Documentation", "text": "Return to the guides overview.", "href": "/docs/documentation/overview"},
                    {"title": "SDK Reference", "text": "Open the Python SDK reference.", "href": "/docs/sdk-reference/overview"},
                    {"title": "API Reference", "text": "Open the HTTP API reference.", "href": "/docs/api-reference/auth/create-access-token"},
                ]
            ),
        ],
        ["404"],
    )
