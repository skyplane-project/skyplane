#!/bin/bash
# color output
BGreen='\033[1;32m'
BBlue='\033[1;34m'
Blue='\033[0;34m'
NC='\033[0m' # No Color

src=$1
dest=$2
min_range=`skylark experiments util-grid-throughput ${src} ${dest}`
max_range=`skylark experiments get-max-throughput ${src}`
n_instances=${3:-1}
n_samples=${4:-10}

solution_dir=data/tradeoff_sweep/${src}_${dest}_${n_instances}
mkdir -p ${solution_dir}


echo -e "${Blue}Range: [${min_range}, ${max_range}]${NC}"
throughputs=`python -c "import numpy as np; print(' '.join(np.around(np.linspace(${min_range}, ${max_range}*.9, ${n_samples}), decimals=2).astype(str).tolist()[::-1]))"`
for t in ${throughputs}; do
    echo -e "${BGreen}Solving for throughput ${t}...${NC}"
    bash scripts/tradeoff/eval.sh $src $dest $t $n_instances |& tee ${solution_dir}/solve-throughput_${t}.log
    tail -5 ${solution_dir}/solve-throughput_${t}.log > ${solution_dir}/result_${t}.log
    echo -e "${BGreen}Results written to ${solution_dir}/solve-throughput_${t}.log${NC}"
    
    # if notify command exists, call it
    if command -v notify &> /dev/null
    then
        notify "Solve complete for throughput ${t}\n\n`tail -5 ${solution_dir}/solve-throughput_${t}.log`"
    fi
done

skylark deprovision