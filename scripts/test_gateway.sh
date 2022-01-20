#!/bin/bash
set -x
sudo DOCKER_BUILDKIT=1 docker build -t gateway_test .
sudo docker run --rm --ipc=host --network=host --name=skylark_gateway gateway_test python /pkg/skylark/gateway/gateway_daemon.py --chunk-dir /dev/shm/skylark_test/chunks