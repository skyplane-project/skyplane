#!/bin/bash
set -x
sudo DOCKER_BUILDKIT=1 docker build -t gateway_test .
sudo docker run --rm --network=host --mount type=tmpfs,dst=/skylark,tmpfs-size=$(($(free -b  | head -n2 | tail -n1 | awk '{print $2}')/2)) --name=skylark_gateway gateway_test python /pkg/skylark/gateway/gateway_daemon.py --chunk-dir /skylark_test/chunks --region aws:us-east-1 --outgoing-ports "{}"