#!/bin/bash
set -xe
source scripts/pack_docker.sh
mkdir -p data/ray_cluster_configs
sed "s#DOCKERIMAGEHERE#\"$SKYLARK_DOCKER_IMAGE\"#g" scripts/ray_cluster/config_paras.yaml > data/ray_cluster_configs/config_paras_with_docker.yaml
ray up data/ray_cluster_configs/config_paras_with_docker.yaml --use-normal-shells -y
ray rsync_up data/ray_cluster_configs/config_paras_with_docker.yaml ~/.aws/credentials '~/.aws/credentials'
ray rsync_up data/ray_cluster_configs/config_paras_with_docker.yaml data/config.json '/pkg/data/config.json'
# ssh -fNT -L 8265:localhost:8265 skydev

echo "To access the Ray dashboard, run 'ray dashboard data/ray_cluster_configs/config_paras_with_docker.yaml'"