# syntax=docker/dockerfile:1
FROM python:3.11-slim

# install apt packages
RUN --mount=type=cache,target=/var/cache/apt apt-get update \
    && apt-get install --no-install-recommends -y curl ca-certificates stunnel4 gcc libc-dev wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# TODO: uncomment when on-prem is re-enabled
##install HDFS Onprem Packages
#RUN apt-get update && \
#    apt-get install -y openjdk-11-jdk && \
#    apt-get clean 
#
#ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
#
#RUN wget https://archive.apache.org/dist/hadoop/core/hadoop-3.3.0/hadoop-3.3.0.tar.gz -P /tmp \
#    && tar -xzf /tmp/hadoop-3.3.0.tar.gz -C /tmp \
#    && mv /tmp/hadoop-3.3.0 /usr/local/hadoop \
#    && rm /tmp/hadoop-3.3.0.tar.gz
#
#ENV HADOOP_HOME /usr/local/hadoop

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
COPY scripts/requirements-gateway.txt /tmp/requirements-gateway.txt

#Onprem: Install Hostname Resolution for HDFS
#COPY scripts/on_prem/hostname /tmp/hostname

RUN --mount=type=cache,target=/root/.cache/pip pip3 install --no-cache-dir -r /tmp/requirements-gateway.txt && rm -r /tmp/requirements-gateway.txt

WORKDIR /pkg
COPY . .
RUN pip3 install --no-dependencies -e ".[aws,azure,gcp,gateway]"

CMD /etc/init.d/stunnel4 start; python3 /pkg/skyplane/gateway/gateway_daemon.py --chunk-dir /skyplane/chunks --outgoing-ports '{}' --region local
