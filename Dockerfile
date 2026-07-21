
  FROM python:3.11-slim

  WORKDIR /app

  RUN apt-get update \
      && apt-get install -y --no-install-recommends \
        curl \
        git \
        build-essential \
      && rm -rf /var/lib/apt/lists/*

  RUN python -m pip install --no-cache-dir --upgrade pip setuptools wheel

  RUN pip install --no-cache-dir \
        fastapi==0.116.1 \
        uvicorn==0.35.0 \
        requests==2.32.4 \
        numpy==2.3.1 \
        pandas==2.3.1

  RUN mkdir -p /app/data /app/logs \
      && echo "template-ready" > /app/data/template_marker.txt

  COPY <<'PY' /app/main.py
  from fastapi import FastAPI
  import platform
  import pandas as pd
  import numpy as np

  app = FastAPI()

  @app.get("/")
  def root():
      return {
          "status": "ok",
          "python": platform.python_version(),
          "pandas": pd.__version__,
          "numpy": np.__version__,
      }

  @app.get("/health")
  def health():
      return {"ready": True}
  PY

  EXPOSE 8000

  CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]