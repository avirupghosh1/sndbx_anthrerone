# Sandbox backends: Docker (default) and gVisor (`runsc`)

**Remote Linux VM:** to run **all** sandbox containers on a **separate** Linux machine while keeping this repo on your laptop, set **`DOCKER_HOST`** (and TLS/SSH as needed). See **`docs/REMOTE_SANDBOX_VM.md`**. For **Firecracker** KVM microVMs instead of Docker, use **`SANDBOX_ENGINE=firecracker`** on a **Linux** host (often the same Colima VM); see **`docs/FIRECRACKER.md`**.

With **`SANDBOX_ENGINE=docker`** (default), this API uses **Docker Engine** via `docker-py` (`ContainerManager`). You can switch the **OCI runtime** the daemon uses for new sandboxes:

| Mode | Env (typical) | OCI runtime | `runtime` field on sandboxes |
|------|----------------|-------------|------------------------------|
| Default Linux containers | `SANDBOX_ISOLATION=docker` (default) | Daemon default (usually `runc`) | `docker` |
| gVisor user-space kernel | `SANDBOX_ISOLATION=gvisor` (or `runsc` / `gv`) | `runsc` | `gvisor` |
| **Lima / Colima VM** (one QEMU VM per sandbox) | `SANDBOX_ISOLATION=lima` or `colima` or `lima-vm` | *(n/a — not Docker)* | `lima` |

With **`SANDBOX_ENGINE=firecracker`**, sandboxes are **Firecracker microVMs** (not Docker). The `runtime` column is **`firecracker`**; **`SANDBOX_ISOLATION`** is ignored **unless** it selects Lima (Lima wins first — see `execution_backend.py`).

With **`SANDBOX_ISOLATION=lima`** (or `colima`), sandboxes are **Lima VMs** (`limactl`); **`SANDBOX_ENGINE`** is ignored for that process (see `docs/LIMA_SANDBOX.md`).

**Explicit override:** set `SANDBOX_DOCKER_OCI_RUNTIME=runsc` (or `runc` / `default` / `docker` for default). When non-empty, it wins over `SANDBOX_ISOLATION` for choosing the OCI name.

**Prerequisites for gVisor:** `runsc` must be installed and registered on the Docker daemon (`docker info` → Runtimes). Many **Docker Desktop** installs do **not** ship `runsc`; Linux hosts are the common case.

**Warm pool:** changing isolation after the API has filled a warm pool can leave **mixed** idle rows (`docker` vs `gvisor`). Prefer setting isolation before first traffic, or set `SANDBOX_WARM_POOL_SIZE=0` and recreate the API container after a change.

**File writes (`put_archive` / ``docker cp``):** under **gVisor** the API uses **exec + stdin** to ``cat > file`` inside the sandbox instead of ``put_archive``, because ``docker cp`` / archive extract can be unreliable there; see the [gVisor FAQ](https://gvisor.dev/docs/user_guide/faq/).

**File reads (``runsc``):** try **in-container ``base64``** over a **TTY** exec first (small ASCII payload, no tar). Then **``get_archive``**, picking the tar member that matches **``X-Docker-Container-Path-Stat``** ``size``/``name`` so PAX blobs are never returned as file text. Finally **``cat``** with **TTY** exec.

**File writes (``runsc``):** primary path is **chunked shell + ``base64 -d``** (repeated ``exec_run``, no hijacked attach stdin). Hijacked stdin (**multiplex** or **TTY raw**) is unreliable on ``runsc`` and caused **500** when ``cat`` exited non-zero. Multiplexed ``cat >file`` over attach remains only as **fallback** if the shell path fails.

---

## How this differs from a separate “second API backend”

- **Current shipped “second way”:** same `ContainerManager`, same socket, **`containers.run(..., runtime="runsc")`** when enabled. Snapshots, exec, files, and warm pool stay on the **Engine** path (`docker commit` still applies).
- **Future alternative (not implemented):** a **parallel plane** (e.g. direct **containerd** + `runsc`, or a custom rootfs + OCI bundle) that still implements `SandboxExecutionPlane` but does **not** go through Engine—different snapshot/export story, same REST shape if you add a factory in `execution_backend.py`.

On **macOS**, workloads still run inside Docker Desktop’s **Linux VM**; gVisor does not run user code natively on Darwin.
