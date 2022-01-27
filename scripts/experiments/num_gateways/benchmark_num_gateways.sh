#!/bin/bash
# get args from command line
SRC_REGION=$1
DST_REGION=$2
INTER_REGION=$3

NUM_CONNECTIONS=${4:-64}
CHUNK_SIZE_MB=${5:-16}
N_CHUNKS_PER_CONNECTION=${6:-512}

if [ -z "$SRC_REGION" ] || [ -z "$DST_REGION" ] || [ -z "$INTER_REGION" ]; then
    echo "Usage: $0 SRC_REGION DST_REGION INTER_REGION [NUM_GATEWAYS] [CHUNK_SIZE_MB] [N_CHUNKS_PER_CONNECTION]"
    echo "Options include:"
    skylark replicate-random --help
    exit 1
fi

function benchmark_config {
    NUM_CONNECTIONS=$1
    NUM_GATEWAYS=$2
    N_CHUNKS=$((N_CHUNKS_PER_CONNECTION * NUM_CONNECTIONS * NUM_GATEWAYS))
    set -x
    skylark replicate-random $SRC_REGION $DST_REGION $INTER_REGION --chunk-size-mb $CHUNK_SIZE_MB --n-chunks $N_CHUNKS --num-gateways $NUM_GATEWAYS --num-outgoing-connections $NUM_CONNECTIONS --aws-instance-class m5.4xlarge
    set +x
}

# log function with message argument
function log() {
    BGreen='\033[1;32m'
    NC='\033[0m' # No Color
    echo -e "${BGreen}$1${NC}"
}

EXP_ID="$SRC_REGION-$DST_REGION-$(./scripts/utils/get_random_word_hash.sh)"
LOG_DIR=data/experiments/benchmark_num_gateways/logs/$EXP_ID
log "Creating log directory $LOG_DIR"
log "Experiment ID: $EXP_ID"
rm -rf $LOG_DIR
mkdir -p $LOG_DIR
touch $LOG_DIR/launch.log

log "Stopping existing instances"
skylark deprovision &>> $LOG_DIR/launch.log

log "Building docker image"
source scripts/pack_docker.sh &>> $LOG_DIR/launch.log
if [ $? -ne 0 ]; then
    log "Error building docker image"
    exit 1
fi

# assert gnu parallel is installed
if ! [ -x "$(command -v parallel)" ]; then
    log "Error: gnu parallel is not installed"
    exit 1
fi

for NUM_GATEWAYS in 24; do
    log "Running benchmark with $NUM_GATEWAYS gateways"
    benchmark_config $NUM_CONNECTIONS $NUM_GATEWAYS |& tee $LOG_DIR/$NUM_GATEWAYS.log
done

log "Stopping instances"
skylark deprovision &>> $LOG_DIR/launch.log

log "Done, results in $LOG_DIR"
log "Experiment ID: $EXP_ID"