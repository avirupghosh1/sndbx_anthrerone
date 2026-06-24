FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY proxy_service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY proxy_service /app
COPY ["api_server copy/envd_guest", "/app/api_server_envd_guest"]

EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
