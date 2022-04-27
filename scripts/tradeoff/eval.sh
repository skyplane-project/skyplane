#!/bin/bash
set -e
src=$1
dest=$2
throughput_per_instance=$3
max_instance=$4
throughput=`python -c "print(${throughput_per_instance} * ${max_instance})"`

# color output
BGreen='\033[1;32m'
BBlue='\033[1;34m'
Blue='\033[0;34m'
NC='\033[0m' # No Color

key_prefix="fake_imagenet"
bucket_prefix="exps-paras-tradeoff"
experiment=${src//[:]/-}_${dest//[:]/-}_${max_instance}_${throughput}_${key_prefix//[\/]/-}
experiment_dir=data/tradeoff/${experiment}
mkdir -p ${experiment_dir}
filename=${experiment_dir}/plan.json
echo -e "${Blue}Results directory: ${experiment_dir}${NC}"
echo -e "${Blue}Plan file: ${filename}${NC}"

src_bucket=(${src//:/ })
src_bucket=${bucket_prefix}-skylark-${src_bucket[1]}
dest_bucket=(${dest//:/ })
dest_bucket=${bucket_prefix}-skylark-${dest_bucket[1]}
echo -e "${Blue}Source bucket: ${src_bucket}${NC}"
echo -e "${Blue}Destination bucket: ${dest_bucket}${NC}"

echo -e "${Blue}Creating buckets...${NC}"
set -x
python scripts/setup_bucket.py --key-prefix ${key_prefix} --bucket-prefix ${bucket_prefix} --src-data-path ../${key_prefix}/ --src-region ${src} --dest-region ${dest} &> ${experiment_dir}/setup_bucket.log
{ set +x; } 2>/dev/null
echo -e "${Blue}Building docker image${NC}"
source scripts/pack_docker.sh &> ${experiment_dir}/pack_docker.log

echo -e "${Blue}Solving for throughput...${NC}"
set -x
skylark solver solve-throughput ${src} ${dest} ${throughput} -o ${filename} --max-instances ${max_instance} |& tee ${experiment_dir}/solve-throughput.log
{ set +x; } 2>/dev/null
echo -e "${Blue}Throughput plan written to ${filename}${NC}"

echo -e "${Blue}Replicating data${NC}"
skylark replicate-json ${filename} \
    --source-bucket $src_bucket \
    --dest-bucket $dest_bucket \
    --reuse-gateways \
    --src-key-prefix ${key_prefix} \
    --dest-key-prefix ${key_prefix} |& tee ${experiment_dir}/replicate-json.log

echo -e "${Blue}Solver problem and solution:${NC}"
tail -2 ${experiment_dir}/solve-throughput.log
echo -e "${Blue}Resulting throughput:${NC}"
tail -1 ${experiment_dir}/replicate-json.log