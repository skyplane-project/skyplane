# syntax=docker/dockerfile:1
FROM ghcr.io/parasj/skylark-docker-base:main
COPY . /pkg
RUN pip install /pkg