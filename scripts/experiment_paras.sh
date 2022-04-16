#!/bin/bash
set -xe

src=$1
dest=$2

key_prefix="fake_imagenet"
bucket_prefix="exps-paras"
src_bucket=(${src//:/ })
src_bucket=${bucket_prefix}-skylark-${src_bucket[1]}
dest_bucket=(${dest//:/ })
dest_bucket=${bucket_prefix}-skylark-${dest_bucket[1]}
echo $src_bucket
echo $dest_bucket
max_instance=1
experiment=${src//[:]/-}_${dest//[:]/-}_${max_instance}_${key_prefix//[\/]/-}
filename=data/plan/${experiment}.json
echo $filename

# creats buckets + bucket data and sets env variables
python scripts/setup_bucket.py --key-prefix ${key_prefix} --bucket-prefix ${bucket_prefix} --src-data-path ../${key_prefix}/ --src-region ${src} --dest-region ${dest}

# TODO:artificially increase the number of chunks
# TODO: try synthetic data

source scripts/pack_docker.sh

## create plan
throughput=$(($max_instance*3))
# throughput=25
skylark solver solve-throughput ${src} ${dest} ${throughput}  -o ${filename} --max-instances ${max_instance};
echo ${filename}

# make exp directory
mkdir -p data/results
mkdir -p data/results/${experiment}

# save copy of plan
cp ${filename} data/results/${experiment}

## run replication (random)
#skylark replicate-json ${filename} \
#   --use-random-data \
#   --size-total-mb 73728 \
#   --n-chunks 1152 &> data/results/${experiment}/random-logs.txt
#tail -1 data/results/${experiment}/random-logs.txt;

# run replication (obj store)
skylark replicate-json ${filename} \
    --source-bucket $src_bucket \
    --dest-bucket $dest_bucket \
    --reuse-gateways \
    --src-key-prefix ${key_prefix} \
    --dest-key-prefix ${key_prefix} |& tee data/results/${experiment}/obj-store-logs.txt
tail -1 data/results/${experiment}/obj-store-logs.txt
echo ${experiment}