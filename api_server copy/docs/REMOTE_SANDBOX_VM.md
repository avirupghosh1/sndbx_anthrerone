# Running sandbox containers on a separate Linux VM (repo unchanged on the host)

This API **does not embed a hypervisor** and **does not** create the Linux VM for you. It talks to **one Docker Engine API** (`docker-py`). To keep your **git repo and API process on macOS (or anywhere)** while **every sandbox container** runs on a **dedicated Linux VM**, you point that API at the VM’s daemon using **`DOCKER_HOST`** (and optional TLS/SSH env vars that `docker-py` already understands). On macOS, **Colima** is a common choice: **`DOCKER_HOST`** is usually the **Unix socket** printed by **`colima docker-env`**, not Multipass and not necessarily **`ssh://`**. **Firecracker** mode (`SANDBOX_ENGINE=firecracker`) is separate: microVMs run on a **Linux + KVM** host (often the same Colima Linux VM as Docker); see **`docs/FIRECRACKER.md`**.

**gVisor:** install **`runsc` on the Linux VM only** (where `dockerd` runs). Toggle **`SANDBOX_ISOLATION=gvisor`** on the **API process** when you want new sandboxes to use `runtime=runsc` on **that same daemon**. The API code path is unchanged; only the **target daemon** and **OCI runtime** change.

---

## 1. What runs where (be precise)

| Component | Typical location | Role |
|-----------|------------------|------|
| Your repo, Python, FastAPI | macOS (or CI, or another VM) | Serves HTTP; **orchestration logic**; SQLite DB path you configure |
| **Docker Engine (`dockerd`)** | **Linux VM** | Creates/starts/stops **all** sandbox containers, pulls images, **`docker commit`** snapshots |
| **`runsc` (gVisor)** | **Same Linux VM as `dockerd`** | Optional OCI runtime; must appear under **Runtimes** in `docker info` **on the VM** |
| Sandbox **container filesystems** | **Inside the Linux VM** | Never on macOS directly |

The API sends **Engine API** calls (create, exec, commit, …) to whatever **`DOCKER_HOST`** selects. **All** workload containers created by this server therefore live on **that** daemon—i.e. on the VM if `DOCKER_HOST` points there.

---

## 2. Linux VM setup (once)

**Step-by-step (Colima on macOS, or Multipass/cloud + SSH):** see **`docs/REMOTE_SANDBOX_VM_SETUP.md`**.

Summary:

1. Create a **Linux x86_64 or arm64** VM (cloud, Multipass, UTM, ESXi, etc.) with a **fixed reachable address** from the machine that runs the API (hostname or IP).
2. On the **VM**, install **Docker Engine** (official Docker docs for your distro).
3. On the **VM**, install and register **gVisor** when you need it, e.g. [gVisor install](https://gvisor.dev/docs/user_guide/install/) (`runsc` + `runsc install`, restart Docker). Confirm:

   ```bash
   docker info    # lists runsc under Runtimes
   docker run --rm --runtime=runsc hello-world
   ```

4. Expose the daemon to the API **safely** (pick one; avoid anonymous TCP on the public internet):

   - **SSH (often simplest for dev):** Docker supports `DOCKER_HOST=ssh://user@vm`. Ensure SSH keys and `docker` group on the VM.
   - **TLS on TCP:** configure `dockerd` with TLS certs; set `DOCKER_HOST`, `DOCKER_TLS_VERIFY`, `DOCKER_CERT_PATH` per Docker remote client docs.

   Plain **`tcp://` without TLS** is only acceptable on a **trusted** network (e.g. private VPC + firewall).

---

## 3. API / repo configuration (no code fork)

On the **machine that runs `python main.py`** (or your API container), set:

```bash
# Required: Docker daemon on the Linux VM
export DOCKER_HOST=ssh://ubuntu@192.0.2.10
# or: export DOCKER_HOST=tcp://192.0.2.10:2376  (+ TLS env vars if you use TLS)

# Optional: default Linux containers vs gVisor on *that* daemon
export SANDBOX_ISOLATION=docker    # default OCI (usually runc) on the VM
# export SANDBOX_ISOLATION=gvisor  # use runsc on the VM (runsc must be installed there)
```

You can put the same variables in **`api_server/.env`**; `main.py` loads that file **before** `Config` is read.

**Important:** If the API runs **inside a Docker container** on your Mac, do **not** mount only `/var/run/docker.sock` from the Mac and expect sandboxes on the VM—**mounting the Mac socket sends traffic to Mac Docker**, not the VM. For the VM workflow, pass **`DOCKER_HOST`** (and TLS/SSH settings) **into the API container’s environment** instead (or run the API on the host / in the Linux VM).

---

## 4. Switching “gVisor mode” vs “plain Docker”

- **Plain Linux containers on the VM:** `SANDBOX_ISOLATION=docker` (or unset) and leave `SANDBOX_DOCKER_OCI_RUNTIME` empty (or set to `runc` / `default` / `docker` per `config.py`).
- **gVisor on the VM:** `SANDBOX_ISOLATION=gvisor` (or `SANDBOX_DOCKER_OCI_RUNTIME=runsc`). **Only affects newly created containers** on the daemon pointed at by `DOCKER_HOST`.

Changing isolation after a **warm pool** has provisioned sandboxes can leave mixed idle sandboxes (`docker` vs `gvisor` runtime label). Prefer one isolation mode per steady state, or set `SANDBOX_WARM_POOL_SIZE=0` and restart the API after a change.

---

## 5. Database and paths

- **SQLite (`DATABASE_PATH`)** usually lives **with the API** (host or API container). That is fine: rows store **remote** `container_id` strings; the API resolves them via **remote** Engine.
- **Images and layers** for sandboxes live on the **VM’s** Docker storage, not on the Mac (unless `DOCKER_HOST` still points at Mac Docker).

---

## 6. Mental model (one sentence)

**Your repo stays intact:** configure **`DOCKER_HOST`** so the existing `ContainerManager` drives a **Linux VM’s** Docker; install **`runsc` on that VM**; flip **`SANDBOX_ISOLATION`** on the API when you want **`runsc`** instead of the default OCI for **new** sandbox containers.

See also: `SANDBOX_BACKENDS_FUTURE.md`.
