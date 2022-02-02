src="aws:us-east-1"
dest="aws:us-west-1"
bucket_prefix="sarah"
src_bucket=(${src//:/ })
src_bucket=${bucket_prefix}-skylark-${src_bucket[1]}
dest_bucket=(${dest//:/ })
dest_bucket=${bucket_prefix}-skylark-${dest_bucket[1]}
echo $src_bucket
echo $dest_bucket
max_instance=8
experiment=${src//[:]/-}_${dest//[:]/-}
filename=data/plan/${experiment}.json
echo $filename

# creats buckets + bucket data and sets env variables
#python setup_bucket.py --key-prefix "fake_imagenet" --bucket-prefix "sarah" --gcp-project skylark-sarah --src-data-path ../fake_imagenet/ --src-region ${src} --dest-region ${dest}

source scripts/pack_docker.sh;

# create plan
skylark solver solve-throughput ${src} ${dest} 1 -o ${filename} --max-instances ${max_instance};

# setup credentials 
export GOOGLE_APPLICATION_CREDENTIALS="/home/ubuntu/skylark/skylark-sarah-7f8b82af365f.json"

# make exp directory 
mkdir -p data/results
mkdir -p data/results/${experiment}

# run replication (obj store)
skylark replicate-json ${filename} \
    --gcp-project skylark-sarah \
    --source-bucket $src_bucket \
    --dest-bucket $dest_bucket \
    --key-prefix fake_imagenet > data/results/${experiment}/obj-store-logs.txt

# run replication (random)
skylark replicate-json ${filename} \
    --gcp-project skylark-sarah \
    --use_random_data \
    --size_total_mb 73728 \
    --n_chunks 1152  > data/results/${experiment}/random-logs.txt
