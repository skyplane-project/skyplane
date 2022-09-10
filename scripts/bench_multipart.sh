#!/bin/bash

BUCKET=$1
NPARALLEL=$2

for i in `seq 1 $NPARALLEL`; do
    RANDOMUUID=`date +%s%N | cut -c1-13`
    aws s3api create-multipart-upload --bucket $1 --key tmp/$RANDOMUUID &> /dev/null &
done

wait