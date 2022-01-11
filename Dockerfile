# syntax=docker/dockerfile:1
FROM python:3.8-slim
COPY scripts/requirements-gateway.txt /tmp/requirements-gateway.txt
RUN --mount=type=cache,target=/root/.cache/pip pip install --no-cache-dir --compile -r /tmp/requirements-gateway.txt && rm -r /tmp/requirements-gateway.txt

WORKDIR /pkg
COPY . .
RUN pip install -e .
CMD ["python", "skylark/gateway/gateway_daemon.py"]