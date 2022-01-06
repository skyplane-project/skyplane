#!/bin/bash

# color output
BGreen='\033[1;32m'
NC='\033[0m' # No Color

mkdir -p ./dist
echo -e "${BGreen}Building docker image${NC}"
DOCKER_BUILDKIT=1 sudo docker build -t skylark .
# sudo docker save skylark | pv > ./dist/skylark.tar
# du -sh ./dist/skylark.tar

DOCKER_URL="ghcr.io/parasj/skylark:local-$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 8 | head -n 1)"
echo -e "${BGreen}Uploading docker image to $DOCKER_URL${NC}"
sudo docker tag skylark $DOCKER_URL
sudo docker push $DOCKER_URL
sudo docker system prune -f

export SKYLARK_DOCKER_IMAGE=$DOCKER_URL
echo -e "${BGreen}SKYLARK_DOCKER_IMAGE=$SKYLARK_DOCKER_IMAGE${NC}"