#!/bin/bash
sudo docker build -t gateway_test .
sudo docker run --rm --ipc=host --network=host --name=skylark_gateway gateway_test /env/bin/python /pkg/skylark/gateway/gateway_daemon.py