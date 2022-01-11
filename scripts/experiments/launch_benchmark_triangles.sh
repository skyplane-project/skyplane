#!/bin/bash
set -x

function cleanup {
    python skylark/benchmark/stop_all_instances.py --gcp-project $GCP_PROJECT
}
trap cleanup EXIT

bash scripts/experiments/benchmark_triangles.sh "aws:sa-east-1" "aws:us-west-2"
bash scripts/experiments/benchmark_triangles.sh "aws:ap-northeast-1" "aws:eu-central-1"
bash scripts/experiments/benchmark_triangles.sh "aws:ap-southeast-1" "aws:eu-west-1"
bash scripts/experiments/benchmark_triangles.sh "aws:us-east-1" "aws:us-west-1"
bash scripts/experiments/benchmark_triangles.sh "aws:eu-central-1" "aws:us-east-1"