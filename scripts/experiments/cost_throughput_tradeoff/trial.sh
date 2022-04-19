#!/bin/bash

dir="data/cost_throughput_tradeoff/$1_to_$2"
mkdir -p $dir
experiment_hash=`openssl rand -base64 12`
out_file="$dir/$3_${experiment_hash}.log"
skylark replicate-random-solve $1 $2 --num-gateways 1 --num-outgoing-connections 4 -s $((1024)) --reuse-gateways --solve --solver-required-throughput-gbits $3 |& tee $out_file
cat $out_file | grep '{' > "$dir/$3_${experiment_hash}.jsonl"
