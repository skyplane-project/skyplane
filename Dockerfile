# syntax=docker/dockerfile:1
FROM python:3.10-slim

# increase number of open files and concurrent TCP connections
RUN (echo 'net.ipv4.ip_local_port_range = 12000 65535' >> /etc/sysctl.conf) \
    && (echo 'fs.file-max = 1048576' >> /etc/sysctl.conf) \
    && mkdir -p /etc/security/ \
    && (echo '*                soft    nofile          1048576' >> /etc/security/limits.conf) \
    && (echo '*                hard    nofile          1048576' >> /etc/security/limits.conf) \
    && (echo 'root             soft    nofile          1048576' >> /etc/security/limits.conf) \
    && (echo 'root             hard    nofile          1048576' >> /etc/security/limits.conf)

# install apt packages
RUN --mount=type=cache,target=/var/cache/apt apt update \
    && apt-get install --no-install-recommends -y git wget ca-certificates build-essential graphviz \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# install gateway
COPY scripts/requirements-gateway.txt /tmp/requirements-gateway.txt
RUN --mount=type=cache,target=/root/.cache/pip pip3 install --no-cache-dir --compile -r /tmp/requirements-gateway.txt && rm -r /tmp/requirements-gateway.txt

WORKDIR /pkg
COPY . .
RUN pip3 install --no-dependencies -e ".[gateway]"

CMD ["python3", "skylark/gateway/gateway_daemon.py"]
