# syntax=docker/dockerfile:1
FROM python:3.8-slim

# install CoinOR
# ARG DEBIAN_FRONTEND="noninteractive"
# ENV TZ="America/Los_Angeles"
# RUN --mount=type=cache,target=/var/cache/apt apt update \
#     && apt-get install --no-install-recommends -y git wget ca-certificates build-essential gcc g++ gfortran pkg-config libblas-dev liblapack-dev coinor-libcbc-dev libz-dev \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*
# RUN --mount=type=cache,target=/root/.cache/pip pip install --no-cache-dir --compile numpy && pip install --no-cache-dir --compile cylp

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
RUN --mount=type=cache,target=/root/.cache/pip pip install --no-cache-dir --compile -r /tmp/requirements-gateway.txt && rm -r /tmp/requirements-gateway.txt

WORKDIR /pkg
COPY . .
RUN pip install -e .
CMD ["python", "skylark/gateway/gateway_daemon.py"]