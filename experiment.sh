#src="aws:us-east-1"
#dest="aws:us-west-1"

#src="gcp:europe-north1-a"
#dest="gcp:us-west4-a"

src="aws:ap-northeast-2"
dest="gcp:us-central1-a"

bucket_prefix="sarah"
src_bucket=(${src//:/ })
src_bucket=${bucket_prefix}-skylark-${src_bucket[1]}
dest_bucket=(${dest//:/ })
dest_bucket=${bucket_prefix}-skylark-${dest_bucket[1]}
echo $src_bucket
echo $dest_bucket
max_instance=16
experiment=${src//[:]/-}_${dest//[:]/-}_${max_instance}
filename=data/plan/${experiment}.json
throughput=100
echo $filename

# setup credentials 
export GOOGLE_APPLICATION_CREDENTIALS="/home/ubuntu/skylark/skylark-sarah-7f8b82af365f.json"

## creats buckets + bucket data and sets env variables
python setup_bucket.py --key-prefix "fake_imagenet" --bucket-prefix "sarah" --gcp-project skylark-sarah --src-data-path ../fake_imagenet/ --src-region ${src} --dest-region ${dest}


# TODO:artificially increase the number of chunks 
# TODO: try synthetic data 

source scripts/pack_docker.sh;

## create plan
skylark solver solve-throughput ${src} ${dest} 1 -o ${filename} --max-instances ${max_instance};

# make exp directory 
mkdir -p data/results
mkdir -p data/results/${experiment}

# run replication (random)
skylark replicate-json ${filename} \
    --gcp-project skylark-sarah \
    --use-random-data \
    --size-total-mb 73728 \
    --n-chunks 1152  > data/results/${experiment}/random-logs.txt
tail -1 data/results/${experiment}/random-logs.txt;

# run replication (obj store)
skylark replicate-json ${filename} \
    --gcp-project skylark-sarah \
    --source-bucket $src_bucket \
    --dest-bucket $dest_bucket \
    --key-prefix fake_imagenet > data/results/${experiment}/obj-store-logs.txt
tail -1 data/results/${experiment}/obj-store-logs.txt;
