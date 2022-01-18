#!/bin/bash

# color output
BGreen='\033[1;32m'
NC='\033[0m' # No Color

echo -e "${BGreen}Building docker image${NC}"
sudo DOCKER_BUILDKIT=1 docker build -t skylark .

DOCKER_URL="ghcr.io/parasj/skylark:local-$(openssl rand -hex 16)"
echo -e "${BGreen}Uploading docker image to $DOCKER_URL${NC}"
sudo docker tag skylark $DOCKER_URL
sudo docker push $DOCKER_URL
sudo docker system prune -f

export SKYLARK_DOCKER_IMAGE=$DOCKER_URL
echo -e "${BGreen}SKYLARK_DOCKER_IMAGE=$SKYLARK_DOCKER_IMAGE${NC}"
