# syntax=docker/dockerfile:1
FROM ghcr.io/parasj/skylark-docker-base:main
# todo enable BBR by default
COPY . /pkg
RUN pip install /pkg