#!/bin/bash
set -x

function cleanup {
    skylark deprovision
}
trap cleanup EXIT

bash scripts/experiments/num_gateways/benchmark_num_gateways.sh "aws:ap-northeast-1" "aws:eu-central-1" "aws:us-east-2" 64 8 16