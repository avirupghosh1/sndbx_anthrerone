# Lima / Colima VM sandboxes

Set **`SANDBOX_ISOLATION=lima`** or **`SANDBOX_ISOLATION=colima`** (alias) so each API sandbox is a **separate Lima/QEMU virtual machine** created with **`limactl`**, instead of a Docker container.

Colima installs Lima and exposes **`limactl`** on your PATH; this backend does **not** talk to Docker for sandbox lifecycle—it only needs Lima.

## Run the API on the host vs in Docker (Colima / Lima)

**Running the API directly on the Mac (or Linux) host** is often the **simplest** setup when you use Colima + Lima sandboxes:

| Topic | On the **host** | API in **Docker** |
|--------|-----------------|-------------------|
| **`limactl` / PATH** | Same as your shell after Colima/Lima install — **works without `LIMA_REMOTE_HOST`**. | Container image usually has **no** `limactl`; use **`LIMA_REMOTE_HOST`** + SSH to the host, or bake a custom image (still no nested QEMU in the API container). |
| **Nested virtualization** | Not an issue for Lima (QEMU runs on the host). | Avoid running Lima **inside** the API container; delegate to host or remote. |
| **`DOCKER_HOST` for Colima** | Point at Colima’s socket from the host when using **`SANDBOX_ISOLATION=docker`**. | Use `DOCKER_HOST` + mount socket / `host.docker.internal` as in `REMOTE_SANDBOX_VM.md`. |
| **Reproducibility / CI** | Depends on each developer’s host (Python version, brew, etc.). | Image gives **repeatable** builds; closer to production if prod is containerized. |
| **Ops** | You manage restarts (systemd, launchd, `tmux`, or a small process manager). | Orchestrator / restart policies are built in. |
| **Isolation of the API itself** | Weaker unless you use venv + firewall; mis-bound `0.0.0.0` exposes the API. | Network namespace + image boundary (still not a substitute for auth). |

**Bottom line:** for **local development** with **`SANDBOX_ISOLATION=lima`**, running **`uvicorn` on the host** is a **good default** and avoids SSH indirection. For **shared CI or production**, prefer a **defined image** and either **Docker + `LIMA_REMOTE_HOST`** to a dedicated Lima-capable host, or **`SANDBOX_ISOLATION=docker`** + Colima’s Docker only.

**Minimal host run:** use the helper script (creates `api_server/.venv` if needed, default DB `sandboxes.host.db`):

```bash
cd api_server
export SANDBOX_ISOLATION=lima   # or: colima
./scripts/run_api_host.sh
```

Or manually:

```bash
cd api_server
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export SANDBOX_ISOLATION=lima
export DATABASE_PATH="${PWD}/sandboxes.host.db"
uvicorn main:app --reload --host 127.0.0.1 --port 8000
```

## Requirements

- **`limactl`** and QEMU where Lima runs — normally the **same host as the API process**.
- A Lima **template** reachable from that host (default: **`template://ubuntu-24.04`**).
- The default Ubuntu template uses **passwordless `sudo`** for the login user; file and command helpers run `sudo -n …` where needed.

### API runs inside Docker (no nested Lima in the container)

The API container typically has **no `limactl`** and should **not** run QEMU inside the container (nested virt is awkward or unsupported).

**Recommended:** keep Lima on a **real host or VM** and either **run the API on that host** (see above) or let the API call Lima over SSH:

1. Install Lima on a machine reachable from the API container (often your **Mac host** or a small Linux VM).
2. Install an **SSH server** there and authorize the API container (deploy key or shared key).
3. Set:

```bash
SANDBOX_ISOLATION=lima
LIMA_REMOTE_HOST=you@lima-host   # e.g. you@192.168.5.1 or you@host.docker.internal (Mac)
# Optional: identity file and known-hosts policy
LIMA_REMOTE_SSH_EXTRA_ARGS=-i /run/secrets/lima_ssh_key -o UserKnownHostsFile=/dev/null
```

Every `limactl` invocation becomes `ssh … you@lima-host limactl …`. QEMU still runs only on **lima-host**.

**Other options:** run the API **on the host** (not in Docker) where Colima/Lima already live; or use **`SANDBOX_ISOLATION=docker`** + **`DOCKER_HOST`** to talk to Colima’s Docker **inside one shared Linux VM** (containers per sandbox, not one Lima VM per sandbox).

## Environment variables

| Variable | Default | Meaning |
|----------|---------|--------|
| **`SANDBOX_ISOLATION`** | `docker` | Set to **`lima`**, **`colima`**, or **`lima-vm`** to enable this plane. |
| **`LIMA_REMOTE_HOST`** | *(empty)* | If set (e.g. `user@host`), run **`ssh user@host limactl …`** from the API process (required for Dockerized API + host Lima). |
| **`LIMA_REMOTE_LIMACTL_PATH`** | `limactl` | `limactl` binary name/path **on the remote** host. |
| **`LIMA_REMOTE_SSH_EXTRA_ARGS`** | *(empty)* | Extra `ssh` flags (`shlex.split`), e.g. `-i /key -p 22`. |
| **`LIMACTL_PATH`** | `limactl` | Lima CLI binary **when not using** `LIMA_REMOTE_HOST` (local API only). |
| **`LIMA_SANDBOX_TEMPLATE`** | `template://ubuntu-24.04` | Default Lima template when `template_id` is a Docker-style name (e.g. `python:3.11`). |
| **`LIMA_CREATE_EXTRA_ARGS`** | *(empty)* | Extra arguments for `limactl create` (parsed with `shlex.split`). |
| **`LIMA_START_TIMEOUT_SEC`** | `600` | Timeout for `limactl start` / first-boot wait. |
| **`LIMA_SHELL_USE_SUDO`** | `true` | Wrap remote commands in `sudo -n bash -lc …` for the default user. |

## Choosing a guest OS / template

Docker image names like **`node:18`** are **not** pulled as OCI images under Lima. Either:

- Set **`LIMA_SANDBOX_TEMPLATE`** to the Lima template you want for all such sandboxes, or  
- Pass a **`template_id`** that is already a Lima reference, e.g. **`template://docker`** (Docker-in-Lima), **`template://ubuntu-22.04`**, or a path to a **`.yaml`** file.

## Interaction with `SANDBOX_ENGINE`

**`SANDBOX_ISOLATION=lima` is evaluated before `SANDBOX_ENGINE`.** If both `SANDBOX_ENGINE=firecracker` and Lima isolation are set, the API uses **Lima** and logs a warning.

## Features vs Docker engine

| Feature | Lima VMs |
|---------|----------|
| **POST /sandboxes**, commands, files, list, delete | Supported via `limactl shell`. |
| **Warm pool** (`SANDBOX_WARM_POOL_SIZE>0`) | **Disabled** (would provision many full VMs). |
| **Custom templates + `docker commit` warm snapshot** | **Skipped**; DB marker `__lima_vm__` is stored like Firecracker. |
| **`docker commit` snapshots** | **Not available** (no `commit_filesystem_snapshot` on this plane). |

## Instance naming

Sandboxes use Lima instance names **`msbx-` + 12 hex chars** so they are easy to recognize in `limactl list` and to avoid accidental operations on unrelated VMs.
