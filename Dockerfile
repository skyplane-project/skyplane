# syntax=docker/dockerfile:1
FROM python:3.11-slim

# install apt packages
RUN --mount=type=cache,target=/var/cache/apt apt update \
    && apt-get install --no-install-recommends -y curl ca-certificates stunnel4 gcc libc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# configure stunnel
RUN mkdir -p /etc/stunnel \
    && openssl genrsa -out key.pem 2048 \
    && openssl req -new -x509 -key key.pem -out cert.pem -days 1095 -subj "/C=US/ST=California/L=San Francisco" \
    && cat key.pem cert.pem >> /etc/stunnel/stunnel.pem \
    && rm key.pem cert.pem \
    && mkdir -p /usr/local/var/run/ \
    && echo "client = no" >> /etc/stunnel/stunnel.conf \
    && echo "[gateway]" >> /etc/stunnel/stunnel.conf \
    && echo "accept = 8080" >> /etc/stunnel/stunnel.conf \
    && echo "connect = 8081" >> /etc/stunnel/stunnel.conf \
    && echo "cert = /etc/stunnel/stunnel.pem" >> /etc/stunnel/stunnel.conf

# increase number of open files and concurrent TCP connections
RUN (echo 'net.ipv4.ip_local_port_range = 12000 65535' >> /etc/sysctl.conf) \
    && (echo 'fs.file-max = 1048576' >> /etc/sysctl.conf) \
    && mkdir -p /etc/security/ \
    && (echo '*                soft    nofile          1048576' >> /etc/security/limits.conf) \
    && (echo '*                hard    nofile          1048576' >> /etc/security/limits.conf) \
    && (echo 'root             soft    nofile          1048576' >> /etc/security/limits.conf) \
    && (echo 'root             hard    nofile          1048576' >> /etc/security/limits.conf)

# install gateway
RUN --mount=type=cache,target=/root/.cache/pip pip3 install --no-cache-dir 'poetry==1.2.2'
COPY pyproject.toml poetry.lock /tmp/
RUN --mount=type=cache,target=/root/.cache/pip cd /tmp \
    && poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi --no-root --no-root -E gateway --only gateway \
    && rm -rf /tmp/pyproject.toml /tmp/poetry.lock \
    && pip uninstall -y poetry

WORKDIR /pkg
COPY . .
RUN pip3 install --no-dependencies -e ".[aws,azure,gcp,gateway]"

CMD /etc/init.d/stunnel4 start; python3 /pkg/skyplane/gateway/gateway_daemon.py --chunk-dir /skyplane/chunks --outgoing-ports '{}' --region local