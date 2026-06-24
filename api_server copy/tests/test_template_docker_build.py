from __future__ import annotations

import io
import sys
import tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import orchestrator.template_docker_build as builder


class _FakeImages:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def build(self, **kwargs):
        self.calls.append(kwargs)
        dockerfile_text = Path(str(kwargs["path"])).joinpath("Dockerfile").read_text(encoding="utf-8")
        context_file = Path(str(kwargs["path"])).joinpath("app", "hello.txt").read_text(encoding="utf-8")
        return object(), [
            {"stream": f"dockerfile-bytes={len(dockerfile_text)}\n"},
            {"stream": f"context={context_file}\n"},
        ]


class _FakeDockerClient:
    def __init__(self) -> None:
        self.images = _FakeImages()
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _context_tar_bytes() -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        data = b"world\n"
        ti = tarfile.TarInfo("app/hello.txt")
        ti.size = len(data)
        tf.addfile(ti, io.BytesIO(data))
    return buf.getvalue()


def test_build_image_from_dockerfile_uses_remote_docker_sdk(monkeypatch) -> None:
    fake_client = _FakeDockerClient()
    monkeypatch.setattr(builder.docker, "from_env", lambda *args, **kwargs: fake_client)

    tag, log = builder.build_image_from_dockerfile(
        dockerfile="FROM python:3.12-slim\nCOPY app/hello.txt /tmp/hello.txt\n",
        image_tag="repo/test:123",
        template_id="tpl-1",
        build_args={"FOO": "bar", "": "skip"},
        context_tar_gzip=_context_tar_bytes(),
        build_timeout_sec=321,
        embed_envd=False,
    )

    assert tag == "repo/test:123"
    assert "dockerfile-bytes=" in log
    assert "context=world" in log
    assert fake_client.closed is True
    assert len(fake_client.images.calls) == 1
    call = fake_client.images.calls[0]
    assert call["tag"] == "repo/test:123"
    assert call["dockerfile"] == "Dockerfile"
    assert call["buildargs"] == {"FOO": "bar"}
