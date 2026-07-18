import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

_MODULE_PATH = Path(__file__).resolve().parents[1] / "orchestrator" / "k8s_leader_election.py"
_SPEC = importlib.util.spec_from_file_location("k8s_leader_election", _MODULE_PATH)
assert _SPEC and _SPEC.loader
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
KubernetesLeaseClient = _MODULE.KubernetesLeaseClient


class FakeLeaseClient(KubernetesLeaseClient):
    def __init__(self, lease=None, *, identity="api-a", conflict=False):
        super().__init__(
            SimpleNamespace(
                API_SERVICE_INSTANCE_ID=identity,
                RUNTIME_GATEWAY_NAMESPACE="sandboxes",
                WARM_POOL_COORDINATOR_LEASE_NAMESPACE="sandboxes",
                WARM_POOL_COORDINATOR_LEASE_TTL_SEC=15,
            )
        )
        self.base_url = "https://kubernetes"
        self.lease = lease
        self.conflict = conflict
        self.requests = []

    def available(self):
        return True

    def _request(self, method, path, *, body=None):
        self.requests.append((method, path, body))
        if method == "GET":
            if self.lease is None:
                return 404, {}
            return 200, self.lease
        if method == "POST":
            if self.lease is not None:
                return 409, {}
            self.lease = body
            return 201, body
        if method == "PUT":
            if self.conflict:
                return 409, {}
            self.lease = body
            return 200, body
        raise AssertionError(method)


def _lease(holder, renew_time, resource_version="1"):
    return {
        "metadata": {"name": "warm-pool-coordinator", "namespace": "sandboxes", "resourceVersion": resource_version},
        "spec": {
            "holderIdentity": holder,
            "leaseDurationSeconds": 15,
            "acquireTime": renew_time,
            "renewTime": renew_time,
            "leaseTransitions": 0,
        },
    }


def _ts(delta_seconds):
    return (datetime.now(timezone.utc) + timedelta(seconds=delta_seconds)).isoformat().replace("+00:00", "Z")


def test_kubernetes_lease_create_acquires_when_missing():
    client = FakeLeaseClient(lease=None, identity="api-a")

    assert client.try_acquire_or_renew("warm-pool-coordinator") is True

    assert client.lease["spec"]["holderIdentity"] == "api-a"
    assert [request[0] for request in client.requests] == ["GET", "POST"]


def test_kubernetes_lease_rejects_active_other_holder():
    client = FakeLeaseClient(lease=_lease("api-b", _ts(-1)), identity="api-a")

    assert client.try_acquire_or_renew("warm-pool-coordinator") is False

    assert [request[0] for request in client.requests] == ["GET"]


def test_kubernetes_lease_takes_expired_other_holder_with_resource_version():
    client = FakeLeaseClient(lease=_lease("api-b", _ts(-60), resource_version="7"), identity="api-a")

    assert client.try_acquire_or_renew("warm-pool-coordinator") is True

    assert client.lease["metadata"]["resourceVersion"] == "7"
    assert client.lease["spec"]["holderIdentity"] == "api-a"
    assert client.lease["spec"]["leaseTransitions"] == 1
    assert [request[0] for request in client.requests] == ["GET", "PUT"]


def test_kubernetes_lease_conflict_does_not_report_leader():
    client = FakeLeaseClient(lease=_lease("api-b", _ts(-60), resource_version="7"), identity="api-a", conflict=True)

    assert client.try_acquire_or_renew("warm-pool-coordinator") is False

    assert [request[0] for request in client.requests] == ["GET", "PUT"]
