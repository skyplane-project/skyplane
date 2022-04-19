#!/bin/bash

# color output
BGreen='\033[1;32m'
BBlue='\033[1;34m'
Blue='\033[0;34m'
NC='\033[0m' # No Color

# usage:
# ./run_all_trials.sh <src> <dst> <t1> <t2> ... <tN>

src=$1
dst=$2
throughputs="${@:3}"

for t in $throughputs; do
    for trial in {1..1}; do
        echo -e "${BGreen}Running trial $trial for throughput $t${NC}"
        bash scripts/experiments/cost_throughput_tradeoff/trial.sh $src $dst $t |& tee /tmp/out.log
        
        # call notify function if in path
        if [ -x "$(command -v notify)" ]; then
            notify "Throughput $t finished trial $trial of 5\n`tail -1 /tmp/out.log`"
        fi
    done
done

skylark deprovision