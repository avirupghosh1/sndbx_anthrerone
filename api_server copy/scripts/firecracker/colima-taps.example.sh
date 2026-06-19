#!/usr/bin/env bash
# Example: create tap interfaces tapfc0..tapfc7 on an existing bridge `br-fc`
# with IP 172.16.0.1/24 on the bridge (run with sudo inside Colima/Linux).
#
#   sudo BRIDGE=br-fc ./scripts/firecracker/colima-taps.example.sh
set -euo pipefail
BRIDGE="${BRIDGE:-br-fc}"
PREFIX="${PREFIX:-tapfc}"
COUNT="${COUNT:-8}"
BASE_IP="${BASE_IP:-172.16.0.1/24}"

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run as root (sudo)." >&2
  exit 1
fi

ip link show "$BRIDGE" >/dev/null 2>&1 || ip link add "$BRIDGE" type bridge
ip addr add "$BASE_IP" dev "$BRIDGE" 2>/dev/null || true
ip link set "$BRIDGE" up

for i in $(seq 0 $((COUNT - 1))); do
  dev="${PREFIX}${i}"
  if ! ip link show "$dev" >/dev/null 2>&1; then
    ip tuntap add dev "$dev" mode tap
  fi
  ip link set "$dev" master "$BRIDGE"
  ip link set "$dev" up
  echo "ok $dev -> $BRIDGE"
done

echo "Enable NAT (example): sysctl -w net.ipv4.ip_forward=1"
echo "  iptables -t nat -A POSTROUTING -s 172.16.0.0/24 -j MASQUERADE"
