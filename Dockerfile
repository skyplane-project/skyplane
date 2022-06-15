# syntax=docker/dockerfile:1
FROM python:3.10-slim

# install apt packages
RUN --mount=type=cache,target=/var/cache/apt apt update \
    && apt-get install --no-install-recommends -y curl ca-certificates stunnel4 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# configure stunnel
RUN mkdir -p /etc/stunnel \
    && mkdir -p /usr/local/var/run \
    && echo "client = no" >> /etc/stunnel/stunnel.conf \
    && echo "pid = /usr/local/var/run/stunnel.pid" >> /etc/stunnel/stunnel.conf \
    && echo "[gateway]" >> /etc/stunnel/stunnel.conf \
    && echo "accept = 8080" >> /etc/stunnel/stunnel.conf \
    && echo "connect = 127.0.0.1:8081" >> /etc/stunnel/stunnel.conf \
    && /etc/init.d/stunnel4 start

# increase number of open files and concurrent TCP connections
RUN (echo 'net.ipv4.ip_local_port_range = 12000 65535' >> /etc/sysctl.conf) \
    && (echo 'fs.file-max = 1048576' >> /etc/sysctl.conf) \
    && mkdir -p /etc/security/ \
    && (echo '*                soft    nofile          1048576' >> /etc/security/limits.conf) \
    && (echo '*                hard    nofile          1048576' >> /etc/security/limits.conf) \
    && (echo 'root             soft    nofile          1048576' >> /etc/security/limits.conf) \
    && (echo 'root             hard    nofile          1048576' >> /etc/security/limits.conf)

# install gateway
COPY scripts/requirements-gateway.txt /tmp/requirements-gateway.txt
RUN --mount=type=cache,target=/root/.cache/pip pip3 install --no-cache-dir -r /tmp/requirements-gateway.txt && rm -r /tmp/requirements-gateway.txt

WORKDIR /pkg
COPY . .
RUN pip3 install --no-dependencies -e ".[gateway]"

CMD ["python3", "skyplane/gateway/gateway_daemon.py"]