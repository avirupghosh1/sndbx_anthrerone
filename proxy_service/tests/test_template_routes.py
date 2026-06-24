from __future__ import annotations

import sys
from pathlib import Path

from starlette.applications import Starlette
from starlette.routing import Route
from starlette.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import template_routes


def _test_app() -> Starlette:
    return Starlette(
        routes=[
            Route("/internal/templates/build/dockerfile", template_routes.build_dockerfile, methods=["POST"]),
            Route(
                "/internal/templates/build/dockerfile/stream",
                template_routes.build_dockerfile_stream,
                methods=["POST"],
            ),
        ]
    )


def test_internal_dockerfile_build_route_uses_gateway_builder(monkeypatch) -> None:
    monkeypatch.setattr(template_routes, "internal_api_key_valid", lambda request: True)

    def _fake_build(**kwargs):
        assert kwargs["template_id"] == "tpl-1"
        assert kwargs["dockerfile"].startswith("FROM python")
        return "repo/test:123", "step one\nstep two\n"

    monkeypatch.setattr(template_routes, "build_image_from_dockerfile", _fake_build)

    client = TestClient(_test_app())
    resp = client.post(
        "/internal/templates/build/dockerfile",
        json={
            "template_id": "tpl-1",
            "dockerfile": "FROM python:3.12-slim\n",
            "build_mode": "parsed",
        },
    )

    assert resp.status_code == 200
    assert resp.json()["image_tag"] == "repo/test:123"
    assert resp.json()["effective_mode"] == "docker_cli"


def test_internal_dockerfile_build_stream_route_emits_log_and_result(monkeypatch) -> None:
    monkeypatch.setattr(template_routes, "internal_api_key_valid", lambda request: True)

    def _fake_stream(**kwargs):
        assert kwargs["template_id"] == "tpl-stream"
        yield {"type": "log", "line": "step one\n"}
        yield {"type": "result", "image_tag": "repo/test:stream", "build_log": "step one\n"}

    monkeypatch.setattr(template_routes, "stream_build_image_from_dockerfile", _fake_stream)

    client = TestClient(_test_app())
    resp = client.post(
        "/internal/templates/build/dockerfile/stream",
        json={
            "template_id": "tpl-stream",
            "dockerfile": "FROM python:3.12-slim\n",
            "build_mode": "kaniko",
        },
    )

    assert resp.status_code == 200
    assert '"type": "log"' in resp.text
    assert '"effective_mode": "docker_cli"' in resp.text
