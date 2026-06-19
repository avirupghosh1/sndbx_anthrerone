# Sandbox API Server & Orchestrator

> **Quickstart (run API, SDK, WebSocket):** use the repository **[README.md](../README.md)** in the parent folder.  
> This document is the **long-form** reference (architecture, every endpoint, deployment).

---

Complete REST API server with Docker container orchestration and agent runtime system.

**Isolation:** default **`SANDBOX_ENGINE=docker`**: Linux containers on Docker Engine, optionally **gVisor** (`SANDBOX_ISOLATION=gvisor`). **`SANDBOX_ISOLATION=lima`** (or **`colima`**) uses **one Lima/QEMU VM per sandbox** via `limactl` (see `docs/LIMA_SANDBOX.md` — for Colima+Lima, run **`./scripts/run_api_host.sh`** on the host instead of containerizing the API). Set **`SANDBOX_ENGINE=firecracker`** for **KVM microVMs** on a Linux host (see `docs/FIRECRACKER.md`). See `docs/SANDBOX_BACKENDS_FUTURE.md` for the full matrix. For a **separate Linux VM** for Docker, see `docs/REMOTE_SANDBOX_VM.md` and `DOCKER_HOST`. **Colima / Multipass setup:** `docs/REMOTE_SANDBOX_VM_SETUP.md`.

**E2B drop-in (agentlib / Custodian-style WebSocket + `AsyncSandbox` shim):** see `docs/E2B_DROPIN_TESTING.md`, `docs/E2B_DROP_IN_IMPLEMENTATION.md`, and **`docs/AGENTLIB_AND_CHECK_CODE.md`** (why PyPI `agentlib` ≠ `check_Code`’s imports). **Envd-style in-guest data plane (Phase 1 HTTP):** `docs/ENVD_STYLE_RUNTIME.md`, `envd_guest/`, `GET /sandboxes/{id}/envd-connection`. **Repo-wide architecture** (API vs shim vs execution planes): **`../docs/ARCHITECTURE.md`**.

## Table of Contents

1. [Architecture](#architecture)
2. [Quick Start](#quick-start)
3. [Components](#components)
4. [API Endpoints](#api-endpoints)
5. [Container Orchestration](#container-orchestration)
6. [Agent Runtime](#agent-runtime)
7. [Configuration](#configuration)
8. [Deployment](#deployment)
9. [Examples](#examples)

---

## Architecture

```
┌─────────────────────────────────────────────┐
│         Client (Python SDK)                  │
│  from my_sdk import Sandbox                 │
│  sandbox = Sandbox.create(api_url=...)      │
└────────────────┬────────────────────────────┘
                 │ REST API (HTTP/HTTPS)
                 ▼
┌─────────────────────────────────────────────┐
│         FastAPI Server (main.py)            │
│  - Authentication (API keys)                │
│  - Request validation                       │
│  - Error handling                           │
└────────┬────────────────┬────────────────────┘
         │                │
    ┌────▼────┐      ┌────▼────┐
    │ Handlers │      │Middleware│
    │ - Sandbox│      │ - Auth   │
    │ - Command│      │ - Errors │
    │ - Files  │      └──────────┘
    │ - Agents │
    └────┬────┘
         │
    ┌────▼─────────────────────────┐
    │   Orchestrator              │
    │  ├─ SandboxManager          │
    │  ├─ ContainerManager        │
    │  └─ AgentRuntime            │
    └────┬──────────────────────────┘
         │
    ┌────▼─────────────────────────┐
    │  Docker Engine              │
    │  (Container Management)     │
    └────┬──────────────────────────┘
         │
         ▼
    ┌─────────────────────────┐
    │ Containers (Sandboxes)  │
    │ - python:3.11           │
    │ - node:18               │
    │ - custom images         │
    └─────────────────────────┘
```

---

## Quick Start

### 1. Prerequisites

- **Docker**: Must be installed and running
- **Python 3.9+**: For API server (`requirements.txt` pins **Pydantic ≥2.12** so `pip install` works on **3.12.4+ / 3.13 / 3.14**; older `pydantic==2.5` could fail building `pydantic-core`)
- **Docker for Mac/Windows**: Ensure Docker daemon is accessible

### 2. Clone and Setup

```bash
cd api_server
chmod +x setup.sh
./setup.sh
```

### 3. Configure Environment

Edit `.env`:
```env
API_KEY=your-secure-key-here
DEBUG=false
DEFAULT_TEMPLATE=python:3.11
DATABASE_PATH=sandboxes.db

# Optional: Docker-only warm pool — pre-create N idle sandboxes so POST /sandboxes can return
# immediately when template/cpu/memory/timeout match this profile (see docs/E2B_COMPARISON.md).
# SANDBOX_WARM_POOL_SIZE=2
# SANDBOX_WARM_POOL_TEMPLATE_ID=python:3.11
# SANDBOX_WARM_POOL_CPU=1
# SANDBOX_WARM_POOL_MEMORY=512m
# SANDBOX_WARM_POOL_TIMEOUT=3600

# Optional: one-time custom template build containers (see docs/CUSTOM_TEMPLATES.md)
# TEMPLATE_BUILD_CPU=2
# TEMPLATE_BUILD_MEMORY=2g

# Optional: gVisor — requires runsc on the Docker daemon (see docs/SANDBOX_BACKENDS_FUTURE.md)
# SANDBOX_ISOLATION=gvisor
# SANDBOX_DOCKER_OCI_RUNTIME=runsc
```

### 4. Start Server

```bash
source venv/bin/activate
python main.py
```

Server runs on `http://localhost:8000`

API docs at `http://localhost:8000/docs`

### 5. Test with SDK

```python
from my_sdk import Sandbox

# Create sandbox
sandbox = Sandbox.create(
    api_url="http://localhost:8000",
    api_key="your-api-key"
)

# Run command
result = sandbox.commands.run("python -c 'print(\"Hello\")'")
print(result.stdout)  # Hello

# Write file
sandbox.files.write("/tmp/test.txt", "content")

# Read file
content = sandbox.files.read("/tmp/test.txt")
print(content)  # content

# Cleanup
sandbox.kill()
```

---

## Components

### 1. ContainerManager (`orchestrator/container_manager.py`)

Manages Docker container lifecycle.

**Key Methods:**
- `create_container(name, config)` - Create container
- `run_command(container_id, command, ...)` - Execute command
- `read_file(container_id, path)` - Read file from container
- `write_file(container_id, path, content)` - Write file to container (Docker: **Engine `put_archive`**, same idea as `docker cp` — not limited by **exec argv / ARG_MAX**; the API server still holds the full body in memory for the request)
- `list_files(container_id, path)` - List directory contents
- `delete_file(container_id, path, recursive)` - Delete file/directory
- `create_directory(container_id, path, mode)` - Create directory
- `get_container_stats(container_id)` - Get resource usage
- `kill_container(container_id, force)` - Kill container
- `is_container_running(container_id)` - Check status

**Resource Management:**
```python
# CPU limits
cpu_limit="1"      # 1 CPU
cpu_limit="0.5"    # 0.5 CPU
cpu_limit="2"      # 2 CPUs

# Memory limits
memory_limit="512m"    # 512 MB
memory_limit="1g"      # 1 GB
memory_limit="4g"      # 4 GB
```

### 2. SandboxManager (`orchestrator/sandbox_manager.py`)

High-level sandbox abstraction over containers.

**Key Methods:**
- `create_sandbox(template_id, metadata, ...)` - Create new sandbox
- `get_sandbox(sandbox_id)` - Get sandbox info
- `list_sandboxes(limit, offset)` - List all sandboxes
- `is_running(sandbox_id)` - Check if running
- `run_command(sandbox_id, command, ...)` - Execute command
- `read_file(sandbox_id, path)` - Read file
- `write_file(sandbox_id, path, content)` - Write file
- `list_files(sandbox_id, path)` - List directory
- `delete_file(sandbox_id, path, recursive)` - Delete file
- `create_directory(sandbox_id, path, mode)` - Create directory
- `get_metrics(sandbox_id)` - Get resource metrics
- `kill_sandbox(sandbox_id, force)` - Kill sandbox
- `pause_sandbox(sandbox_id)` - Pause sandbox
- `resume_sandbox(sandbox_id)` - Resume sandbox

### 3. Agent Runtime (`agents/runtime.py`)

Runs pseudo-agents inside sandboxes.

**Key Methods:**
- `spawn_agent(sandbox_id, agent_name, agent_code, config)` - Spawn agent
- `get_agent(agent_id)` - Get agent
- `send_agent_message(agent_id, message_type, content)` - Send message
- `list_agents(sandbox_id)` - List agents
- `kill_agent(agent_id, force)` - Kill agent
- `pause_agent(agent_id)` - Pause agent
- `resume_agent(agent_id)` - Resume agent
- `get_agent_status(agent_id)` - Get agent status
- `get_agent_messages(agent_id, limit)` - Get messages

**Agent Features:**
- Python code execution
- Message-based communication
- Auto-restart on failure
- Resource limits
- State management

### 4. Database (`database/store.py`)

SQLite database for persistence.

**Tables:**
- `sandboxes` - Sandbox metadata
- `agents` - Agent instances
- `agent_messages` - Message history
- `commands_history` - Command execution history

---

## API Endpoints

### Sandbox Endpoints

**Create Sandbox**
```
POST /sandboxes
X-API-Key: your-api-key

{
  "template_id": "python:3.11",
  "metadata": {"purpose": "testing"},
  "cpu_limit": "1",
  "memory_limit": "512m",
  "timeout": 3600
}

Response 201:
{
  "sandbox_id": "sb-abc123...",
  "state": "running",
  "created_at": "2024-01-01T00:00:00Z",
  "updated_at": "2024-01-01T00:00:00Z",
  "metadata": {"purpose": "testing"},
  "container_id": "abc123..."
}
```

**Get Sandbox**
```
GET /sandboxes/{sandbox_id}
X-API-Key: your-api-key

Response 200: Same as create response
```

**List Sandboxes**
```
GET /sandboxes?limit=100&offset=0
X-API-Key: your-api-key

Response 200:
[
  {sandbox_info},
  ...
]
```

**Kill Sandbox**
```
POST /sandboxes/{sandbox_id}/kill
X-API-Key: your-api-key

Response 200:
{
  "success": true,
  "sandbox_id": "sb-abc123"
}
```

**Pause Sandbox**
```
POST /sandboxes/{sandbox_id}/pause
X-API-Key: your-api-key

Response 200:
{
  "success": true,
  "sandbox_id": "sb-abc123"
}
```

**Resume Sandbox**
```
POST /sandboxes/{sandbox_id}/resume
X-API-Key: your-api-key

Response 200:
{
  "success": true,
  "sandbox_id": "sb-abc123"
}
```

**Get Metrics**
```
GET /sandboxes/{sandbox_id}/metrics
X-API-Key: your-api-key

Response 200:
{
  "sandbox_id": "sb-abc123",
  "memory_usage": 52428800,
  "memory_limit": 536870912,
  "cpu_percent": 5.23,
  "uptime": 10
}
```

### Command Endpoints

**Run Command**
```
POST /sandboxes/{sandbox_id}/commands/run
X-API-Key: your-api-key

{
  "command": "python script.py",
  "cwd": "/app",
  "env": {"DEBUG": "true"},
  "timeout": 30,
  "user": "root"
}

Response 200:
{
  "exit_code": 0,
  "stdout": "output...",
  "stderr": "",
  "pid": 1234,
  "execution_time": 0.523
}
```

**Run command (streaming stdout/stderr, Server-Sent Events)**

Same JSON body as **Run Command**, but use:

```
POST /sandboxes/{sandbox_id}/commands/run/stream
X-API-Key: your-api-key
Content-Type: application/json

{ "command": "for i in 1 2 3; do echo step$i; sleep 1; done", "cwd": "/", "timeout": 120 }
```

Response: `200` with `Content-Type: text/event-stream`. Each event is one line `data: <json>\n\n` where `<json>` has `"type":"stdout"|"stderr"|"error"|"exit"`; stdout/stderr events include `"chunk"` text; the final event is `"type":"exit"` with `"exit_code"`. The server uses Docker Engine `exec_start` with `stream=True`.

**List Command History**
```
GET /sandboxes/{sandbox_id}/commands?limit=100
X-API-Key: your-api-key

Response 200:
{
  "commands": [
    {
      "command_id": "cmd-123",
      "command": "python script.py",
      "exit_code": 0,
      "stdout": "...",
      "stderr": "",
      "execution_time": 0.5,
      "created_at": "2024-01-01T00:00:00Z"
    }
  ]
}
```

### File Endpoints

**List Files**
```
GET /sandboxes/{sandbox_id}/files?path=/tmp
X-API-Key: your-api-key

Response 200:
{
  "path": "/tmp",
  "entries": [
    {
      "path": "/tmp/file.txt",
      "name": "file.txt",
      "type": "file",
      "size": 1024,
      "permissions": 420,
      "modified_at": "2024-01-01T00:00:00Z"
    }
  ]
}
```

**Read File**
```
GET /sandboxes/{sandbox_id}/files/read?path=/tmp/file.txt
X-API-Key: your-api-key

Response 200:
{
  "path": "/tmp/file.txt",
  "content": "file contents..."
}
```

**Write File**
```
POST /sandboxes/{sandbox_id}/files/write
X-API-Key: your-api-key

{
  "path": "/tmp/file.txt",
  "content": "new content",
  "encoding": "utf-8"
}

Response 200:
{
  "path": "/tmp/file.txt",
  "bytes_written": 11,
  "success": true
}
```

**Delete File**
```
POST /sandboxes/{sandbox_id}/files/delete
X-API-Key: your-api-key

{
  "path": "/tmp/file.txt",
  "recursive": false
}

Response 200:
{
  "success": true,
  "path": "/tmp/file.txt"
}
```

**Create Directory**
```
POST /sandboxes/{sandbox_id}/files/mkdir
X-API-Key: your-api-key

{
  "path": "/tmp/newdir",
  "mode": 493
}

Response 200:
{
  "success": true,
  "path": "/tmp/newdir"
}
```

### Agent Endpoints

**Spawn Agent**
```
POST /sandboxes/{sandbox_id}/agents/spawn
X-API-Key: your-api-key

{
  "agent_name": "echo_agent",
  "agent_code": "print('Hello from agent')",
  "config": {"debug": true},
  "auto_start": true
}

Response 201:
{
  "agent_id": "agent-abc123",
  "agent_name": "echo_agent",
  "state": "running",
  "created_at": "2024-01-01T00:00:00Z",
  "config": {"debug": true}
}
```

**List Agents**
```
GET /sandboxes/{sandbox_id}/agents
X-API-Key: your-api-key

Response 200:
{
  "agents": [
    {
      "agent_id": "agent-123",
      "sandbox_id": "sb-123",
      "agent_name": "echo_agent",
      "state": "running"
    }
  ]
}
```

**Get Agent**
```
GET /sandboxes/{sandbox_id}/agents/{agent_id}
X-API-Key: your-api-key

Response 200:
{
  "agent_id": "agent-123",
  "agent_name": "echo_agent",
  "state": "running",
  "created_at": "2024-01-01T00:00:00Z",
  "config": {"debug": true}
}
```

**Kill Agent**
```
POST /sandboxes/{sandbox_id}/agents/{agent_id}/kill
X-API-Key: your-api-key

{
  "agent_id": "agent-123",
  "force": false
}

Response 200:
{
  "success": true,
  "agent_id": "agent-123"
}
```

**Send Agent Message**
```
POST /sandboxes/{sandbox_id}/agents/{agent_id}/messages
X-API-Key: your-api-key

{
  "agent_id": "agent-123",
  "message_type": "task",
  "content": {"task": "analyze", "data": "..."}
}

Response 200:
{
  "success": true,
  "agent_id": "agent-123"
}
```

**Get Agent Messages**
```
GET /sandboxes/{sandbox_id}/agents/{agent_id}/messages?limit=100
X-API-Key: your-api-key

Response 200:
{
  "agent_id": "agent-123",
  "messages": [
    {
      "message_type": "task",
      "content": {"task": "analyze"},
      "timestamp": 1234567890.0
    }
  ]
}
```

---

## Container Orchestration

### Why Docker Engine for sandboxes

| Aspect | Notes |
|--------|--------|
| **Startup** | Depends on image pull; warm pool can hide create latency |
| **Isolation** | Linux namespaces + cgroups; optional **gVisor** via `SANDBOX_ISOLATION=gvisor` (see `docs/SANDBOX_BACKENDS_FUTURE.md`) |
| **Dev setup** | `docker pull` + socket access from the API process |
| **Debugging** | `docker exec`, Engine API, normal container tooling |

For **gVisor (`runsc`)** vs default OCI and notes on a future non-Engine plane, see `docs/SANDBOX_BACKENDS_FUTURE.md`.

All containers have strict resource limits:

```python
# CPU limits (converted to Docker quota)
"1"   -> 100000 microseconds per 100ms = 1 CPU
"0.5" -> 50000 microseconds per 100ms = 0.5 CPU
"2"   -> 200000 microseconds per 100ms = 2 CPUs

# Memory limits (native Docker format)
"512m" -> 512 megabytes
"1g"   -> 1 gigabyte
"4g"   -> 4 gigabytes
```

### Container Images

Any Docker image works as a template:

```bash
# Python
docker pull python:3.11
docker pull python:3.10
docker pull python:3.9

# Node
docker pull node:18
docker pull node:20

# Custom
docker build -t my-custom-image .
docker push my-registry/my-custom-image
```

---

## Agent Runtime

### What Are Agents?

Pseudo-agents are lightweight Python processes running inside sandboxes that:
- Execute user-provided code
- Receive/respond to messages
- Maintain state
- Can interact with sandbox filesystem
- Auto-restart on failure

### Example Agent

```python
# agent_code.py
import time

# Agent runs in loop
while True:
    # Agent logic here
    print("Agent running...")
    time.sleep(1)
```

### Spawning Agents

```python
from my_sdk import Sandbox

sandbox = Sandbox.create()

# Spawn agent
agent_response = sandbox.spawn_agent(
    agent_name="my_agent",
    agent_code="""
import time
print("Agent started")
while True:
    time.sleep(1)
    print("Agent tick")
""",
    config={"debug": True}
)

agent_id = agent_response.agent_id

# Send message to agent
sandbox.send_agent_message(
    agent_id=agent_id,
    message_type="task",
    content={"task": "process", "data": "..."}
)

# Get agent status
status = sandbox.get_agent(agent_id)
print(f"Agent state: {status['state']}")

# Get agent messages
messages = sandbox.get_agent_messages(agent_id, limit=10)

# Kill agent
sandbox.kill_agent(agent_id)
```

### Agent Message Types

```python
# Standard message format
{
    "agent_id": "agent-123",
    "message_type": "task|status|control|data",
    "content": {...},
    "timestamp": "2024-01-01T00:00:00Z"
}

# Example messages
message_type="task"     # content: {"task": "analyze", "data": "..."}
message_type="status"   # content: {"status": "idle|running|done"}
message_type="control"  # content: {"command": "pause|resume|stop"}
message_type="data"     # content: {"key": "value", ...}
```

---

## Configuration

### Environment Variables

```bash
# Server
HOST=0.0.0.0              # Server host
PORT=8000                 # Server port
DEBUG=false               # Debug mode
API_KEY=test-key-12345    # API authentication key

# Database
DATABASE_PATH=sandboxes.db    # SQLite database path

# Docker (optional: point at a Linux VM — see docs/REMOTE_SANDBOX_VM.md)
DOCKER_HOST=               # Docker daemon URL (optional, e.g. ssh://user@host)

# Sandbox defaults
DEFAULT_TEMPLATE=python:3.11      # Default container image
DEFAULT_CPU_LIMIT=1               # Default CPU limit
DEFAULT_MEMORY_LIMIT=512m         # Default memory limit
DEFAULT_TIMEOUT=3600              # Default sandbox timeout

# OCI isolation (Docker Engine only): docker (default) vs gvisor (runsc)
SANDBOX_ENGINE=docker
# SANDBOX_ENGINE=firecracker   # Linux+KVM; see docs/FIRECRACKER.md
SANDBOX_ISOLATION=docker
# SANDBOX_DOCKER_OCI_RUNTIME=   # optional: runsc | runc | default | docker

# Logging
LOG_LEVEL=INFO            # Log level
```

### .env File

**Canonical template:** ``.env.example`` in this directory lists every variable (E2B drop-in, envd, Firecracker, Lima). Copy it to ``.env`` and set ``E2B_DROPIN_WS_SECRET`` before using ``GET …/e2b-connection`` / ``WS …/agent-ws``. Client-side env for SDK and scripts lives in the repo root ``../.env.example``.

```bash
# Create .env file
cat > .env << EOF
API_KEY=your-secure-api-key
DEBUG=false
DATABASE_PATH=sandboxes.db
DEFAULT_TEMPLATE=python:3.11
DEFAULT_CPU_LIMIT=1
DEFAULT_MEMORY_LIMIT=512m
LOG_LEVEL=INFO
EOF
```

---

## Deployment

### Docker Compose (Recommended)

```bash
# Build (or rebuild) the image and start — use --build whenever Python code changed
docker compose up -d --build

# If you only ran `up -d` before, the container may still be an old image (missing new routes).
# Then: docker compose build api --no-cache && docker compose up -d
# Legacy Compose v1: same commands with `docker-compose` instead of `docker compose`.

# View logs
docker compose logs -f api

# Stop
docker compose down

# Clean up (removes named volume with DB)
docker compose down -v
```

### Standalone Docker

```bash
# Build image
docker build -t sandbox-api .

# Run container
docker run -d \
  -p 8000:8000 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e API_KEY=your-key \
  -e DATABASE_PATH=/data/sandboxes.db \
  --name sandbox-api \
  sandbox-api

# View logs
docker logs -f sandbox-api
```

### Kubernetes

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: sandbox-api-config
data:
  API_KEY: "your-key"
  DATABASE_PATH: "/data/sandboxes.db"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sandbox-api
spec:
  replicas: 3
  selector:
    matchLabels:
      app: sandbox-api
  template:
    metadata:
      labels:
        app: sandbox-api
    spec:
      containers:
      - name: api
        image: sandbox-api:latest
        ports:
        - containerPort: 8000
        envFrom:
        - configMapRef:
            name: sandbox-api-config
        volumeMounts:
        - name: docker-sock
          mountPath: /var/run/docker.sock
      volumes:
      - name: docker-sock
        hostPath:
          path: /var/run/docker.sock
---
apiVersion: v1
kind: Service
metadata:
  name: sandbox-api
spec:
  type: LoadBalancer
  ports:
  - port: 8000
  selector:
    app: sandbox-api
```

### Production Checklist

- [ ] Use strong API key
- [ ] Enable HTTPS/TLS
- [ ] Set DEBUG=false
- [ ] Configure logging aggregation
- [ ] Set up monitoring/alerts
- [ ] Implement rate limiting
- [ ] Use database backups
- [ ] Configure resource limits
- [ ] Set up health checks
- [ ] Plan disaster recovery

---

## Examples

### Example 1: Basic Workflow

```python
from my_sdk import Sandbox

# Create sandbox
sandbox = Sandbox.create(
    api_url="http://localhost:8000",
    api_key="test-key-12345"
)

# Run command
result = sandbox.commands.run("echo 'Hello World'")
print(f"Exit code: {result.exit_code}")
print(f"Output: {result.stdout}")

# Write file
sandbox.files.write("/tmp/test.py", """
print("Hello from Python")
""")

# Run Python script
result = sandbox.commands.run("python /tmp/test.py")
print(result.stdout)

# Cleanup
sandbox.kill()
```

### Example 2: Concurrent Operations

```python
import asyncio
from my_sdk import AsyncSandbox

async def run_test(name):
    sandbox = await AsyncSandbox.create(
        api_url="http://localhost:8000",
        api_key="test-key-12345"
    )
    
    result = await sandbox.commands.run(f"echo 'Test {name}'")
    print(f"{name}: {result.stdout}")
    
    await sandbox.kill()

async def main():
    # Create 10 sandboxes in parallel
    tasks = [run_test(f"Test-{i}") for i in range(10)]
    await asyncio.gather(*tasks)

asyncio.run(main())
```

### Example 3: Agent-Based System

```python
from my_sdk import Sandbox

sandbox = Sandbox.create(
    api_url="http://localhost:8000",
    api_key="test-key-12345"
)

# Spawn data processor agent
processor = sandbox.spawn_agent(
    agent_name="processor",
    agent_code="""
import json
import time

config = {}
while True:
    print(f"Processing: {json.dumps(config)}")
    time.sleep(1)
""",
    config={"batch_size": 10}
)

# Send data to agent
sandbox.send_agent_message(
    agent_id=processor.agent_id,
    message_type="data",
    content={"batch": [1, 2, 3, 4, 5]}
)

# Check agent status
status = sandbox.get_agent(processor.agent_id)
print(f"Agent status: {status['state']}")

# Get messages
messages = sandbox.get_agent_messages(processor.agent_id)
print(f"Message count: {len(messages)}")

# Cleanup
sandbox.kill_agent(processor.agent_id)
sandbox.kill()
```

### Example 4: File Operations

```python
from my_sdk import Sandbox

sandbox = Sandbox.create(
    api_url="http://localhost:8000",
    api_key="test-key-12345"
)

# Create directory
sandbox.files.create_directory("/app/data")

# Upload multiple files
files = {
    "/app/data/file1.txt": "Content 1",
    "/app/data/file2.txt": "Content 2",
    "/app/data/file3.txt": "Content 3",
}

for path, content in files.items():
    sandbox.files.write(path, content)

# List files
entries = sandbox.files.list("/app/data")
for entry in entries.entries:
    print(f"{entry.name}: {entry.size} bytes")

# Read file
content = sandbox.files.read("/app/data/file1.txt")
print(content)

# Delete file
sandbox.files.delete("/app/data/file1.txt")

# Cleanup
sandbox.kill()
```

---

## Troubleshooting

### Docker Not Running

```bash
# Check Docker
docker ps

# Start Docker (macOS)
open -a Docker

# Start Docker (Linux)
sudo systemctl start docker
```

### Container Creation Fails

```bash
# Check logs
docker logs sandbox-api

# Check available images
docker images

# Pull missing image
docker pull python:3.11
```

### Database Locked

```bash
# Remove database and restart
rm sandboxes.db
python main.py
```

### Out of Memory

```bash
# Check system memory
docker system df

# Clean up unused containers/images
docker system prune -a
```

---

## Performance Metrics

### Benchmark Results (on modern machine)

| Operation | Time |
|-----------|------|
| Create sandbox | 300-500ms |
| Run command | 50-100ms |
| Read file | 10-20ms |
| Write file | 10-20ms |
| Kill sandbox | 100-200ms |
| Spawn agent | 200-300ms |

### Resource Usage

| Resource | Per Sandbox |
|----------|------------|
| Memory | 5-50MB (varies by image) |
| CPU | <1% idle, up to configured limit |
| Disk | 50-500MB (varies by image) |

---

## License

Same as parent project.

---

**Ready to go! 🚀**
