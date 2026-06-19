# Firecracker microVM sandboxes (`SANDBOX_ENGINE=firecracker`)

This mode runs **Linux KVM microVMs** via the [Firecracker](https://firecracker-microvm.github.io/) `firecracker` binary instead of Docker containers. **Warm pool**, **templates metadata**, and **REST routes** are unchanged; only the **execution plane** swaps.

When the API uses **Lima VM sandboxes** (`SANDBOX_ISOLATION=lima` …), Firecracker is not selected; see `execution_backend.py` and `docs/LIMA_SANDBOX.md`.

## Requirements (all on the **Linux** machine that runs the API)

| Requirement | Notes |
|-------------|--------|
| **Linux + `/dev/kvm`** | Firecracker is not supported on macOS directly. Run the API **inside Colima** (`colima ssh`), a Linux VM, or bare metal. |
| **`firecracker` binary** | e.g. `/usr/local/bin/firecracker` from [releases](https://github.com/firecracker-microvm/firecracker/releases). |
| **Uncompressed `vmlinux`** | Guest kernel image path (`FIRECRACKER_KERNEL`). |
| **ext4 rootfs** with **sshd** | Path (`FIRECRACKER_ROOTFS`). Your SSH **private** key on the API host must match **`authorized_keys`** baked into the image for `FIRECRACKER_SSH_USER` (usually `root`). |
| **TAP devices** | One tap per concurrent VM slot, e.g. `tapfc0` … `tapfc7` on a bridge with routing/NAT to the guest subnet. See scripts below. |
| **`ssh` and `scp` on PATH** | Used for commands and file writes. |

## Environment variables

| Variable | Default | Meaning |
|----------|---------|---------|
| `SANDBOX_ENGINE` | `docker` | Set to `firecracker` (aliases: `fc`, `microvm`). |
| `FIRECRACKER_BINARY` | `/usr/local/bin/firecracker` | Firecracker executable. |
| `FIRECRACKER_KERNEL` | *(required)* | Host path to guest `vmlinux`. |
| `FIRECRACKER_ROOTFS` | *(required)* | Host path to golden **ext4** rootfs (copied per VM). |
| `FIRECRACKER_GATEWAY` | `172.16.0.1` | Guest default gateway (host bridge side). |
| `FIRECRACKER_SUBNET_PREFIX` | `172.16.0` | First three octets; last octet = `FIRECRACKER_GUEST_OCTET_BASE + slot`. |
| `FIRECRACKER_GUEST_OCTET_BASE` | `10` | Starting last octet for slot `0`. |
| `FIRECRACKER_TAP_PATTERN` | `tapfc{slot}` | Tap interface name; `{slot}` = `0 … FIRECRACKER_TAP_SLOTS-1`. |
| `FIRECRACKER_TAP_SLOTS` | `8` | Rotate slots for new VMs; **each tap must exist**. |
| `FIRECRACKER_SSH_USER` | `root` | SSH user in the guest. |
| `FIRECRACKER_SSH_KEY` | *(required)* | Path to **private** SSH key for the guest. |
| `FIRECRACKER_SSH_KNOWN_HOSTS` | `/dev/null` | Passed to `ssh`/`scp` (dev default). |
| `FIRECRACKER_ENABLE_PCI` | `false` | Set `true` only if you pass `--enable-pci` to Firecracker. |
| `FIRECRACKER_ROOTFS_FAST_COPY` | `true` | On Linux, try ``cp --reflink=auto`` before a full ``shutil.copy2`` of the ext4 (fast on btrfs/xfs CoW). Set `false` to always full-copy. |
| `FIRECRACKER_SSH_POLL_SEC` | `0.25` | Sleep between SSH probes after `InstanceStart` (clamped `0.05`…`2`). |
| `FIRECRACKER_SNAPSHOT_DIR` | ``<cwd>/fc-snapshots`` | Directory for full VM snapshot bundles (subfolder per snapshot). |
| `FIRECRACKER_DOCKERFILE_ROOTFS_DIR` | ``<cwd>/fc-dockerfile-rootfs`` | Output directory for **Dockerfile → ext4** template builds (`POST /templates/from-dockerfile`). |
| `FIRECRACKER_DOCKERFILE_EXT4_MIN_MB` | `4096` | Minimum ext4 size (MB) when exporting an OCI image. |
| `FIRECRACKER_DOCKERFILE_EXT4_MAX_MB` | `65536` | Maximum ext4 size (MB) cap. |
| `FIRECRACKER_EXT4_BUILDER_IMAGE` | `alpine:3.19` | Privileged one-shot image for `mkfs.ext4` + `tar` import. |
| `FIRECRACKER_DOCKERFILE_INJECT_SSH_PUBKEY` | `true` | Append `FIRECRACKER_SSH_KEY`’s public key to `/root/.ssh/authorized_keys` inside the exported ext4. |

`SANDBOX_ISOLATION` / gVisor apply **only** to `SANDBOX_ENGINE=docker`.

## Warm pool

`MultiWarmSandboxPool` is unchanged. Each warm sandbox is still a normal `POST /sandboxes` provisioning path via `_create_sandbox_fresh`.

**Docker template warm snapshots** (`docker commit` OCI tags) are **not** used as the guest disk under Firecracker **unless** you registered the template via **`POST /templates/from-dockerfile`**, which exports the built image to a host **`.ext4`** and stores that path in `warm_snapshot_image`. Otherwise registered templates get a sentinel warm marker; guests boot from **`FIRECRACKER_ROOTFS`** (or a per-request `.ext4` path when `from_snapshot_image` points to a host file).

Keep **`SANDBOX_WARM_POOL_SIZE ≤ FIRECRACKER_TAP_SLOTS`** (and ≤ number of real taps) so each idle sandbox has its own tap/IP.

**Faster pool fill:** set **`SANDBOX_WARM_POOL_PROVISION_CONCURRENCY`** (default `1`) to a small integer (e.g. `2`–`4`) so each segment provisions multiple sandboxes in parallel. Stay within tap slots and host CPU/IO; overlapping boots increase peak load on disk (rootfs copy) and KVM.

Example tap + bridge script (run with **sudo** on the Linux host): `scripts/firecracker/colima-taps.example.sh`.

## Networking sketch (inside Colima / Linux)

1. Create a bridge (once), e.g. `br-fc` with `172.16.0.1/24`.
2. For each slot `n` in `0..N-1`:

   ```bash
   sudo ip tuntap add dev tapfc$n mode tap
   sudo ip link set tapfc$n master br-fc
   sudo ip link set tapfc$n up
   ```

3. Enable IPv4 forwarding + NAT from `br-fc` to your uplink (same steps as any Linux router).

4. Guest kernel boot uses `ip=<guest>::<gw>:255.255.255.0::eth0:off` (see `firecracker_plane.py`).

Adjust addresses if you use a different subnet.

## Colima workflow (summary)

1. `colima ssh` into the Linux VM.  
2. Install Firecracker + assets; create taps + bridge as above.  
3. Build or copy an ext4 rootfs with `sshd` and your public key in `/root/.ssh/authorized_keys`.  
4. Export env vars and run the API **in that same VM** (or ensure the process can reach the same paths and `/dev/kvm`).

You can still use **`DOCKER_HOST`** on your Mac for **Docker-backed** dev; Firecracker mode is typically enabled only on the Linux host where KVM is available.

## Full VM snapshots

`POST /sandboxes/{sandbox_id}/snapshot` pauses the microVM, calls Firecracker **`PUT /snapshot/create`** (guest RAM + device state), copies the writable **`rootfs.ext4`** into a bundle under **`FIRECRACKER_SNAPSHOT_DIR`**, resumes the VM, and stores an **`image_ref`** of the form **`fc-bundle:<url-encoded-absolute-path>`**.

Create a **new** sandbox from that ref:

```json
POST /sandboxes
{ "from_snapshot_image": "fc-bundle:/abs/path/…", "template_id": "python:3.11" }
```

Each bundle contains `vm.snap`, `vm.mem`, `rootfs.ext4`, and `manifest.json` (tap slot + guest IP). Cold boots use **`path_on_host: "rootfs.ext4"`** with Firecracker **`cwd`** = the per-VM workdir so snapshots stay portable across workdirs. Restore uses **`PUT /snapshot/load`** with **`network_overrides`** when supported; otherwise it falls back to the tap recorded in the snapshot (only one live VM should use that tap).

**Requirements:** Firecracker version compatible with the snapshot format; enough disk for memory + state + rootfs. See upstream [snapshot-support](https://github.com/firecracker-microvm/firecracker/blob/main/docs/snapshotting/snapshot-support.md).

**Disk vs RAM:** The bundle’s `rootfs.ext4` is a host copy of the virtio disk taken after `PUT /snapshot/create`. The guest’s **full RAM** is in `vm.mem`. For data on the **root ext4** (e.g. under `/root`, `/var`), keep guest caches flushed (`sync` / `blockdev --flushbufs`) before snapshot — the API does this before pause. **`/tmp` is often tmpfs** (RAM-only): it is still in `vm.mem`, but some images run **systemd tmp cleanup** after resume, so **`/tmp` can look empty** even when the snapshot worked. For demos, write under **`/root`** or **`/var/tmp`** on the root disk, or verify with `read_file` on a path you control.

## Dockerfile templates (`POST /templates/from-dockerfile`)

When `SANDBOX_ENGINE=firecracker`, the API still uses **Docker Engine** on the host to **build** the image (`parsed` or `docker_cli` mode), then runs **`docker export`** + a **privileged** helper container to produce an **ext4** under `FIRECRACKER_DOCKERFILE_ROOTFS_DIR`. That file path is stored as `warm_snapshot_image` so new sandboxes boot your custom root.

**Requirements:** `docker` on `PATH`, `DOCKER_HOST` if needed, pull access for `FIRECRACKER_EXT4_BUILDER_IMAGE`, and enough disk. The guest image should include **`openssh-server`** (or equivalent) so `sshd` can start; by default the API **injects** the public half of `FIRECRACKER_SSH_KEY` into `/root/.ssh/authorized_keys` (disable with `FIRECRACKER_DOCKERFILE_INJECT_SSH_PUBKEY=false` if your image already matches your key).

## Limitations

- **Docker** `docker commit` OCI tags are not passed directly to Firecracker as the root disk; use **`from-dockerfile`** export (`.ext4`) or `fc-bundle:` / a host `.ext4` path.  
- **`POST /templates`** one-time warm builds that rely on `docker commit` inside the sandbox plane remain skipped for Firecracker (sentinel marker); use **`from-dockerfile`** or a prebuilt `.ext4` / bundle.  
- Pause/resume uses Firecracker `PATCH /vm` (`Paused` / `Resumed`).

## See also

- `docs/SANDBOX_BACKENDS_FUTURE.md` — comparison with Docker + gVisor.  
- `docs/REMOTE_SANDBOX_VM.md` — running Docker (and this API) on a Linux VM from macOS.
