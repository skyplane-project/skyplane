#!/bin/bash
set -xe
source scripts/pack_docker.sh
mkdir -p data/ray_cluster_configs
sed "s#DOCKERIMAGEHERE#\"$SKYLARK_DOCKER_IMAGE\"#g" scripts/ray_cluster/config_paras.yaml > data/ray_cluster_configs/config_paras_with_docker.yaml
ray up data/ray_cluster_configs/config_paras_with_docker.yaml --use-normal-shells -y
# ssh -fNT -L 8265:localhost:8265 skydev