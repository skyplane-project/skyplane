#!/bin/bash

set -xe
mkdir -p data/results_datasync_random_overlay

for pair in "aws:eu-north-1 aws:us-west-2" "aws:ap-northeast-2 aws:ca-central-1" "aws:us-east-1 aws:us-west-2" "aws:ap-northeast-2 aws:us-west-2" "aws:ap-southeast-2 aws:af-south-1" "aws:ap-southeast-2 aws:eu-west-3"; do
    src=$(echo ${pair} | cut -d' ' -f1)
    dest=$(echo ${pair} | cut -d' ' -f2)
    filename=data/results_datasync_random_overlay/plan_${src}_${dest}.json
    throughput=25
    max_instance=8
    skylark solver solve-throughput ${src} ${dest} ${throughput} -o ${filename} --max-instances ${max_instance}

    # run replicate random
    skylark replicate-json ${filename} \
    --gcp-project skylark-333700 \
    --use-random-data \
    --size-total-mb 73728 \
    --n-chunks 1152 > data/results_datasync_random_overlay/random_logs_${src}_${dest}.txt
    tail -1 data/results_datasync_random_overlay/random_logs_${src}_${dest}.txt
done