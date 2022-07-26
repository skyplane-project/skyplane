#!/bin/bash

die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 1 ] || die "1 argument required (your Github username), $# provided"

# color output
BGreen='\033[1;32m'
NC='\033[0m' # No Color

echo -e "${BGreen}Building docker image${NC}"
set -e
sudo DOCKER_BUILDKIT=1 docker build -t skyplane .
set +e

DOCKER_URL="ghcr.io/$1/skyplane:local-$(openssl rand -hex 16)"
echo -e "${BGreen}Uploading docker image to $DOCKER_URL${NC}"
set -e
sudo docker tag skyplane $DOCKER_URL
sudo docker push $DOCKER_URL
sudo docker system prune -f
set +e

export SKYPLANE_DOCKER_IMAGE=$DOCKER_URL
echo -e "${BGreen}SKYPLANE_DOCKER_IMAGE=$SKYPLANE_DOCKER_IMAGE${NC}"
