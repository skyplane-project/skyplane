#!/bin/bash


# color output
BGreen='\033[1;32m'
BRed='\033[1;31m'
NC='\033[0m' # No Color

set -e
[ "$#" -eq 1 ] || (>&2 echo -e "${BRed}1 argument required (your Github username), $# provided${NC}"; exit 1)
set +e

>&2 echo -e "${BGreen}Building docker image${NC}"
set -e
>&2 sudo DOCKER_BUILDKIT=1 docker build -t skyplane .
set +e

DOCKER_URL="ghcr.io/$1/skyplane:local-$(openssl rand -hex 16)"
>&2 echo -e "${BGreen}Uploading docker image to $DOCKER_URL${NC}"
set -e
>&2 sudo docker tag skyplane $DOCKER_URL
>&2 sudo docker push $DOCKER_URL
>&2 sudo docker system prune -f
set +e

>&2 echo -e "${BGreen}SKYPLANE_DOCKER_IMAGE=$DOCKER_URL${NC}"
echo $DOCKER_URL