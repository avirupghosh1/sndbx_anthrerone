# Hands-on: Linux VM + Docker Engine + gVisor (for `REMOTE_SANDBOX_VM.md`)

Your sandbox workloads run in a **Linux environment** with **Docker Engine**. On macOS, **Colima** is a good default: it runs an **Ubuntu** VM (no Multipass, and you do **not** need `sudo` on the Mac to follow this guide). `**sudo`** appears only **inside** that VM (Colima’s user almost always has passwordless sudo there).

**After this guide:** the API on your Mac talks to Docker via `**DOCKER_HOST`** (a **Unix socket** Colima exposes on macOS). `**SANDBOX_ISOLATION=gvisor`** makes **new** sandboxes use `**runsc`** on **Colima’s Linux VM**.

---

## Path 1 — Colima (recommended if you already have Colima)

### Part A — Start Colima and point the Docker CLI at it

1. Start (adjust CPU / memory if you want):
  ```bash
   colima start --cpu 2 --memory 4
  ```
   Non-default profile (example `dev`): use `colima -p dev start` and the same `-p dev` on every `colima` / `colima docker-env` command below.
2. Load Docker env into your **current shell** (required so `docker` and this API hit Colima, not another daemon):
  ```bash
   eval "$(colima docker-env)"
  ```
   To see the exact values (for `**api_server/.env**`), run **without** `eval`:
   You should see something like `export DOCKER_HOST="unix:///Users/you/.colima/default/docker.sock"`. Copy the `**DOCKER_HOST=`** line into `**.env**` so `python main.py` picks it up even when you did not `eval` in that terminal.
3. From the Mac, verify containers run **through Colima**:
  ```bash
   docker run --rm hello-world
  ```

**Docker Engine:** Colima’s `**docker`** runtime already includes Docker inside the Ubuntu VM. You **do not** run `get-docker.sh` there unless you are on an unusual/custom image.

---

### Part B — Install gVisor (`runsc`) **inside** the Colima VM

Run these from your **Mac**; they execute in the VM via `**colima ssh -- …`**.

```bash
colima ssh -- sudo apt-get update
colima ssh -- sudo apt-get install -y apt-transport-https ca-certificates curl gnupg

colima ssh -- bash -c 'curl -fsSL https://gvisor.dev/archive.key | sudo gpg --dearmor -o /usr/share/keyrings/gvisor-archive-keyring.gpg'
colima ssh -- bash -c 'echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/gvisor-archive-keyring.gpg] https://storage.googleapis.com/gvisor/releases release main" | sudo tee /etc/apt/sources.list.d/gvisor.list > /dev/null'

colima ssh -- sudo apt-get update
colima ssh -- sudo apt-get install -y runsc
```

Register the runtime and restart Docker **in the VM**:

```bash
colima ssh -- sudo runsc install
colima ssh -- sudo systemctl restart docker
```

---

### Part C — Confirm gVisor from your Mac

With `**eval "$(colima docker-env)"**` applied (or `**DOCKER_HOST**` set in `.env` to the same socket):

```bash
docker info | grep -i runsc
docker run --rm --runtime=runsc hello-world
```

If the second command succeeds, set `**SANDBOX_ISOLATION=gvisor**` on the API when you want gVisor-backed sandboxes.

---

### Part D — Point this repo’s API at Colima

```bash
cd /path/to/your/repo/api_server
source venv/bin/activate   # if you use a venv
pip install -r requirements.txt
```

Either **each terminal**:

```bash
eval "$(colima docker-env)"
export SANDBOX_ISOLATION=docker   # or: gvisor
python main.py
```

Or `**api_server/.env**` (values from `**colima docker-env**`):

```env
DOCKER_HOST=unix:///Users/YOU/.colima/default/docker.sock
SANDBOX_ISOLATION=gvisor
```

Replace `**YOU**` and `**default**` with your username and Colima profile if different.

**Note:** `DOCKER_HOST=unix://…` does **not** use SSH. The optional `**docker[ssh]`** extra in `requirements.txt` is only needed if you later use `**DOCKER_HOST=ssh://…**` (e.g. a cloud VM).

---

### Part E — Persistence and gotchas (Colima)


| Topic                    | Detail                                                                                                                                                                 |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Survives reboot**      | `runsc` installed in the VM disk usually survives `colima stop` / `colima start`.                                                                                      |
| **Lost on delete**       | `colima delete` removes the VM; repeat **Part B** after a fresh start.                                                                                                 |
| `**containerd` runtime** | If you use `**colima start --runtime containerd`**, this doc’s Docker + `runsc install` flow targets **Docker**; use the `**docker`** runtime for this API as written. |
| **Docker Desktop**       | If `docker run` still hits Docker Desktop, you did not `**eval "$(colima docker-env)"`** and do not have `**DOCKER_HOST**` in `.env` pointing at Colima.               |


---

## Path 2 — Multipass or cloud VM + `DOCKER_HOST=ssh://…`

Use this if you prefer a **separate** Ubuntu VM (Multipass, EC2, UTM, …) and connect with `**DOCKER_HOST=ssh://user@host`**.

1. **VM:** Ubuntu 22.04/24.04, SSH access from your Mac, user in `**docker`** group.
2. **Docker on VM:** if not preinstalled:
  ```bash
   sudo apt-get update && sudo apt-get install -y ca-certificates curl
   curl -fsSL https://get.docker.com -o /tmp/get-docker.sh && sudo sh /tmp/get-docker.sh
   sudo usermod -aG docker ubuntu   # or your SSH user; then re-login
  ```
3. **gVisor on VM:** same apt steps as **Part B** in Path 1, but run **inside** an `ssh` session (not `colima ssh`).
4. **Mac:** `export DOCKER_HOST=ssh://ubuntu@<VM_IP>` (and install deps with `**docker[ssh]`**). Put `**DOCKER_HOST**` in `**api_server/.env**` if you like.

**Multipass-specific:** install Multipass, `multipass launch 24.04 --name sandbox-docker`, copy your `**~/.ssh/id_ed25519.pub`** into `**ubuntu**`’s `**authorized_keys**`, then use `**DOCKER_HOST=ssh://ubuntu@<IPv4 from multipass info>**`.

---

## Troubleshooting (short)


| Symptom                            | What to try                                                                                         |
| ---------------------------------- | --------------------------------------------------------------------------------------------------- |
| `Unknown runtime specified: runsc` | In VM: `sudo runsc install` → `sudo systemctl restart docker` → `docker info` lists `runsc`.        |
| API still uses Docker Desktop      | Set `**DOCKER_HOST**` from `**colima docker-env**` in `**.env**` or `eval` before `python main.py`. |
| `colima ssh` fails                 | `colima status`; `colima start`.                                                                    |
| `ssh://` from Mac fails            | SSH keys, host reachable, `**pip install -r requirements.txt**` (`docker[ssh]`).                    |


---

## References

- [Colima](https://github.com/abiosoft/colima) — `colima docker-env`, `colima ssh`
- [gVisor installation](https://gvisor.dev/docs/user_guide/install/)
- [Docker Engine (Ubuntu)](https://docs.docker.com/engine/install/ubuntu/) — for Path 2 only if Docker not preinstalled
- Conceptual overview: `REMOTE_SANDBOX_VM.md`

