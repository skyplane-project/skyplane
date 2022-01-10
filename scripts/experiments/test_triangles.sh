#!/bin/bash
# get args from command line
SRC_REGION=$1
DST_REGION=$2

NUM_GATEWAYS=${3:-1}
NUM_CONNECTIONS=${4:-128}

if [ -z "$SRC_REGION" ] || [ -z "$DST_REGION" ]; then
    echo "Usage: $0 SRC_REGION DST_REGION [OPTIONS]"
    echo "Options include:"
    python skylark/benchmark/replicate/benchmark_triangles.py --help
    exit 1
fi

# log function with message argument
function log() {
    BGreen='\033[1;32m'
    NC='\033[0m' # No Color
    echo -e "${BGreen}$1${NC}"
}

EXP_ID=$(./scripts/utils/get_random_word_hash.sh)
LOG_DIR=data/experiments/benchmark_triangles/$EXP_ID
log "Creating log directory $LOG_DIR"
rm -rf $LOG_DIR
mkdir -p $LOG_DIR
touch $LOG_DIR/launch.log

# log "Stopping existing instances"
# python skylark/benchmark/stop_all_instances.py &>> $LOG_DIR/launch.log

log "Building docker image"
source scripts/pack_docker.sh &>> $LOG_DIR/launch.log
if [ $? -ne 0 ]; then
    log "Error building docker image"
    exit 1
fi

function run_direct_cmd {
    echo "python skylark/benchmark/replicate/benchmark_triangles.py --log-dir $LOG_DIR $PASS_THROUGH_ARGS --gateway-docker-image $SKYLARK_DOCKER_IMAGE --num-gateways $NUM_GATEWAYS --num-outgoing-connections $NUM_CONNECTIONS $1 $2"
}

function run_direct_cmd_double_conn {
    # multiply num-outgoing-connections by 2
    echo "python skylark/benchmark/replicate/benchmark_triangles.py --log-dir $LOG_DIR $PASS_THROUGH_ARGS --gateway-docker-image $SKYLARK_DOCKER_IMAGE --num-gateways $NUM_GATEWAYS --num-outgoing-connections $(($NUM_CONNECTIONS * 2)) $1 $2"
}

function run_triangles_inter_cmd {
    echo "python skylark/benchmark/replicate/benchmark_triangles.py --inter-region $3 --log-dir $LOG_DIR $PASS_THROUGH_ARGS --gateway-docker-image $SKYLARK_DOCKER_IMAGE --num-gateways $NUM_GATEWAYS --num-outgoing-connections $NUM_CONNECTIONS $1 $2"
}

# assert gnu parallel is installed
if ! [ -x "$(command -v parallel)" ]; then
    log "Error: gnu parallel is not installed"
    exit 1
fi

# make list of commands to run with gnu parallel (one for each inter-region) and save to $PARALLEL_CMD_LIST (one command per line)
PARALLEL_CMD_LIST="$(run_direct_cmd $SRC_REGION $DST_REGION)\n$(run_direct_cmd_double_conn $SRC_REGION $DST_REGION)"
for inter_region in "aws:ap-northeast-1" "aws:ap-northeast-2" "aws:ap-northeast-3" "aws:ap-southeast-1" "aws:ap-southeast-2" "aws:ca-central-1" "aws:eu-central-1" "aws:eu-north-1" "aws:eu-west-1" "aws:eu-west-2" "aws:eu-west-3" "aws:sa-east-1" "aws:us-east-1" "aws:us-east-2" "aws:us-west-1" "aws:us-west-2"; do
    # if inter-region is same as src or dst region, skip
    if [ "$inter_region" == "$SRC_REGION" ] || [ "$inter_region" == "$DST_REGION" ]; then
        continue
    fi
    PARALLEL_CMD_LIST="$PARALLEL_CMD_LIST\n$(run_triangles_inter_cmd $SRC_REGION $DST_REGION $inter_region) &> $LOG_DIR/$inter_region.log"
done
log "Running commands with gnu parallel:"
echo -e "$PARALLEL_CMD_LIST\n"

log "Parallel:"
parallel \
    -j 8 \
    --results $LOG_DIR/parallel_results.txt \
    --joblog $LOG_DIR/parallel_joblog.txt \
    --eta \
    --halt 1 < <(echo -e "$PARALLEL_CMD_LIST")


# log "Stopping instances"
# python skylark/benchmark/stop_all_instances.py &>> $LOG_DIR/launch.log

log "Done, results in $LOG_DIR"
log "Experiment ID: $EXP_ID"

# interesting pairs
# bash scripts/experiments/test_triangles.sh "aws:us-east-1" "aws:us-west-1"
# bash scripts/experiments/test_triangles.sh "aws:ap-northeast-1" "aws:eu-central-1"
# bash scripts/experiments/test_triangles.sh "aws:ap-southeast-1" "aws:eu-west-1"
# bash scripts/experiments/test_triangles.sh "aws:sa-east-1" "aws:us-west-2"

# todo
# bash scripts/experiments/test_triangles.sh "aws:eu-central-1" "aws:us-east-1"