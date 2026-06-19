# Single-stage, no COPY — works with default ``TEMPLATE_DOCKERFILE_BUILD_MODE=parsed`` without a context tar.
# Installs Anthropic Python SDK + deps used by the drop-in integration test (mock WS + optional live API ping).

FROM python:3.11-slim

RUN pip install --no-cache-dir \
    anthropic \
    httpx \
    "websockets>=12,<15"
