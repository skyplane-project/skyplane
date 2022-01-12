#!/bin/bash
set -x

function cleanup {
    python skylark deprovision
}
trap cleanup EXIT

# usage: benchmark_num_gateways.sh SRC_REGION DST_REGION INTER_REGION [NUM_CONNECTIONS] [CHUNK_SIZE_MB] [N_CHUNKS]
bash scripts/experiments/num_connections/benchmark_num_gateways.sh "aws:ap-northeast-1" "aws:eu-central-1" "aws:us-east-2" 64 4 4096