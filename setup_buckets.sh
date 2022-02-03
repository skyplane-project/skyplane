set -ex

tmp_script=`mktemp`

export GOOGLE_APPLICATION_CREDENTIALS="/home/ubuntu/skylark/skylark-sarah-7f8b82af365f.json"

bucket_prefix="experiments"
dest="aws:us-east-1"
#for src_region in "aws:us-east-1" "gcp:europe-north1-a" "aws:eu-south-1" "aws:eu-north-1" "gcp:europe-west4-a" "gcp:asia-southeast1-a" "aws:ap-northeast-2"
#for src_region in "gcp:europe-north1-a" "aws:eu-south-1" "aws:eu-north-1" "gcp:europe-west4-a" "gcp:asia-southeast1-a" "aws:ap-northeast-2"
#for src_region in  "aws:eu-south-1" "aws:eu-north-1" "gcp:europe-west4-a" "gcp:asia-southeast1-a" "aws:ap-northeast-2"
#for src_region in "aws:ap-northeast-2" "gcp:sa-east1-a" "gcp:europe-north1-a" "aws:sa-east-1" "aws:eu-south-1" "aws:eu-north-1" "gcp:europe-west4-a" "gcp:asia-southeast1-a"
for src_region in "aws:sa-east-1" "aws:ap-northeast-2" "gcp:europe-north1-a"
do
    #for folder in "synthetic-fake-imagenet/4_16384" "synthetic-fake-imagenet/8_8192" "synthetic-fake-imagenet/64_1024" "fake_imagenet"
    for folder in "fake_imagenet"
    do
        echo \" python setup_bucket.py --key-prefix ${folder} --bucket-prefix ${bucket_prefix} --gcp-project skylark-sarah --src-data-path ../${folder}/ --src-region ${src_region} --dest-region ${dest}  \" >> $tmp_script
    done
done

cat $tmp_script | xargs -n 1 -P 1 bash -l -c
