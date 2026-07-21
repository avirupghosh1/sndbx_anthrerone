#!/usr/bin/env python3

import os
import sys
from pathlib import Path

import e2b

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "my_sandbox_sdk"))
from my_sdk import Sandbox as MySandbox  # noqa: E402


API_URL = os.environ["API_URL"]
API_KEY = os.environ["API_KEY"]
TEMPLATE_NAME = os.environ.get("TEMPLATE_NAME", "e2b-warmpool-resize-smoke")

DOCKERFILE = """
FROM python:3.11-slim
WORKDIR /app
RUN python3 -m pip install --no-cache-dir fastapi uvicorn
RUN printf '%s\\n' \\
    'from fastapi import FastAPI' \\
    'app = FastAPI()' \\
    '@app.get("/health")' \\
    'def health():' \\
    '    return {"ok": True}' \\
    > /app/server.py
EXPOSE 8000
CMD ["python3", "-m", "uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8000"]
""".strip()


def main():
    e2b_opts = {
        "api_url": API_URL,
        "api_key": API_KEY,
        "validate_api_key": False,
        "request_timeout": 900,
    }

    template = e2b.Template().from_dockerfile(DOCKERFILE)
    template = template.set_start_cmd(
        "python3 -m uvicorn server:app --host 0.0.0.0 --port 8000",
        e2b.wait_for_port(8000),
    )

    build = e2b.Template.build(
        template,
        alias=TEMPLATE_NAME,
        cpu_count=1,
        memory_mb=512,
        skip_cache=True,
        on_build_logs=print,
        **e2b_opts,
    )
    print(f"built template_id={build.template_id} build_id={build.build_id}")

    sandbox = e2b.Sandbox.create(
        TEMPLATE_NAME,
        timeout=3600,
        metadata={"guest_ports": [8000]},
        envs={"PYTHONUNBUFFERED": "1"},
        **e2b_opts,
    )
    print(f"created sandbox_id={sandbox.sandbox_id}")

    response = MySandbox.attach(
        sandbox.sandbox_id,
        api_url=API_URL,
        api_key=API_KEY,
    ).set_warm_pool_size(4)
    print(f"warm pool resized={response}")

    sandbox.kill(**e2b_opts)
    print("sandbox killed")


if __name__ == "__main__":
    main()
