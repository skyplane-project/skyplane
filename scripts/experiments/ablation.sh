#!/usr/local/bin/bash

set -xe

SRC=$1
DEST=$2
CONN=64
VMS=4

skylark replicate-random-solve $SRC $DEST --num-gateways 1 --num-outgoing-connections 1 -s 128 > "scripts/experiments/abalation_experiments/1-1-128mb"
skylark replicate-random-solve $SRC $DEST --num-gateways 1 --num-outgoing-connections 64 -s 8000 > "scripts/experiments/abalation_experiments/1-64-8gb"
skylark deprovision

skylark replicate-random-solve $SRC $DEST --num-gateways 2 --num-outgoing-connections 64 -s 16000 > "scripts/experiments/abalation_experiments/2-64-16gb"
skylark deprovision

skylark replicate-random-solve $SRC $DEST --num-gateways 4 --num-outgoing-connections 64 -s 32000 > "scripts/experiments/abalation_experiments/4-64-32gb"
skylark deprovision

for TPUT in {2..16..2}
do
    skylark replicate-random-solve $SRC $DEST --num-gateways $VMS --num-outgoing-connections $CONN -s 32000 --reuse-gateways --solve --solver-required-throughput-gbits $TPUT > "scripts/experiments/abalation_experiments/solve-${VMS}-${CONN}-${TPUT}gbps-32gb"
done
skylark deprovision