#!/bin/bash
# get args from command line
SRC_REGION=$1
DST_REGION=$2
INTER_REGION=$3

NUM_GATEWAYS=${4:-1}
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
    # N_CHUNKS_PER_CONNECTION * NUM_CONNECTIONS
    N_CHUNKS=$((N_CHUNKS_PER_CONNECTION * NUM_CONNECTIONS))
    echo "skylark replicate-random $SRC_REGION $DST_REGION $INTER_REGION --chunk-size-mb $CHUNK_SIZE_MB --n-chunks $N_CHUNKS --num-gateways $NUM_GATEWAYS --num-outgoing-connections $NUM_CONNECTIONS --no-reuse-gateways"
}

# log function with message argument
function log() {
    BGreen='\033[1;32m'
    NC='\033[0m' # No Color
    echo -e "${BGreen}$1${NC}"
}

EXP_ID="$SRC_REGION-$DST_REGION-$(./scripts/utils/get_random_word_hash.sh)"
LOG_DIR=data/experiments/benchmark_num_connections/logs/$EXP_ID
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

# make list of commands to run with gnu parallel (one for each inter-region) and save to $PARALLEL_CMD_LIST (one command per line)
PARALLEL_CMD_LIST=""
# powers of 2
for NUM_CONNECTIONS in 1 2 4 8 16 32 64 128 144; do
    PARALLEL_CMD_LIST="$PARALLEL_CMD_LIST\n$(benchmark_config $NUM_CONNECTIONS $NUM_GATEWAYS) &> $LOG_DIR/$NUM_CONNECTIONS.log"
done
log "Running commands with gnu parallel:"
echo -e "$PARALLEL_CMD_LIST\n"
echo -e "$PARALLEL_CMD_LIST\n" >> $LOG_DIR/launch.log

log "Parallel:"
parallel -j 8 --results $LOG_DIR/raw_logs --joblog $LOG_DIR/parallel_joblog.txt --eta < <(echo -e "$PARALLEL_CMD_LIST")


log "Stopping instances"
skylark deprovision &>> $LOG_DIR/launch.log

log "Done, results in $LOG_DIR"
log "Experiment ID: $EXP_ID"