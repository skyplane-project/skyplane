# syntax=docker/dockerfile:1
FROM python:3.8-slim

# increase number of open files and concurrent TCP connections
RUN echo 'net.ipv4.ip_local_port_range = 12000 65535' >> /etc/sysctl.conf
RUN echo 'fs.file-max = 1048576' >> /etc/sysctl.conf
RUN mkdir -p /etc/security/
RUN echo '*                soft    nofile          1048576' >> /etc/security/limits.conf
RUN echo '*                hard    nofile          1048576' >> /etc/security/limits.conf
RUN echo 'root             soft    nofile          1048576' >> /etc/security/limits.conf
RUN echo 'root             hard    nofile          1048576' >> /etc/security/limits.conf

COPY scripts/requirements-gateway.txt /tmp/requirements-gateway.txt
RUN --mount=type=cache,target=/root/.cache/pip pip install --no-cache-dir --compile -r /tmp/requirements-gateway.txt && rm -r /tmp/requirements-gateway.txt

WORKDIR /pkg
COPY . .
RUN pip install -e .
CMD ["python", "skylark/gateway/gateway_daemon.py"]