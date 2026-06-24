from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import orchestrator.runtime_gateway_templates as gateway_templates


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict | None = None, text: str = "") -> None:
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self) -> dict:
        return self._payload


class _FakeClient:
    def __init__(self, response: _FakeResponse) -> None:
        self._response = response
        self.calls: list[dict] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def post(self, url: str, *, json: dict, headers: dict):
        self.calls.append({"url": url, "json": json, "headers": headers})
        return self._response


def test_build_dockerfile_template_via_gateway_posts_to_runtime_gateway(monkeypatch) -> None:
    fake = _FakeClient(_FakeResponse(200, {"image_tag": "repo/test:123", "build_log": "ok\n"}))
    monkeypatch.setattr(gateway_templates.httpx, "Client", lambda *args, **kwargs: fake)
    cfg = SimpleNamespace(
        RUNTIME_GATEWAY_URL="http://runtime-gateway:8080",
        RUNTIME_GATEWAY_API_KEY="secret",
        TEMPLATE_DOCKER_BUILD_TIMEOUT_SEC=900,
    )

    out = gateway_templates.build_dockerfile_template_via_gateway(
        cfg,
        template_id="tpl-1",
        dockerfile="FROM python:3.12-slim\n",
        image_tag=None,
        build_args={"FOO": "bar"},
        context_tar_gzip_base64=None,
        build_mode="parsed",
        embed_envd=True,
    )

    assert out["image_tag"] == "repo/test:123"
    assert fake.calls[0]["url"] == "http://runtime-gateway:8080/internal/templates/build/dockerfile"
    assert fake.calls[0]["headers"]["X-API-Key"] == "secret"
    assert fake.calls[0]["json"]["build_mode"] == "parsed"


def test_build_template_snapshot_via_gateway_raises_runtime_error_on_http_error(monkeypatch) -> None:
    fake = _FakeClient(_FakeResponse(400, {"detail": "boom"}))
    monkeypatch.setattr(gateway_templates.httpx, "Client", lambda *args, **kwargs: fake)
    cfg = SimpleNamespace(
        RUNTIME_GATEWAY_URL="http://runtime-gateway:8080",
        RUNTIME_GATEWAY_API_KEY="secret",
    )

    try:
        gateway_templates.build_template_snapshot_via_gateway(
            cfg,
            template_id="tpl-1",
            base_image="python:3.12-slim",
            env={},
            start_cmd="",
            settle_seconds=0,
            ready_cmd="",
            embed_envd=True,
            envd_pip_timeout_sec=300.0,
            snapshot_repo="mysandbox-snap",
        )
    except RuntimeError as ex:
        assert "boom" in str(ex)
    else:
        raise AssertionError("expected RuntimeError")
