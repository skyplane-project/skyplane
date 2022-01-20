#!/bin/bash
set -x

function cleanup {
    skylark deprovision
}
trap cleanup EXIT

# usage: benchmark_num_connections.sh SRC_REGION DST_REGION INTER_REGION [NUM_GATEWAYS] [CHUNK_SIZE_MB] [N_CHUNKS]
bash scripts/experiments/num_connections/benchmark_num_connections.sh "aws:ap-northeast-1" "aws:eu-central-1" "aws:us-east-2" 1 8 16