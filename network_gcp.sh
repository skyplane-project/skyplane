#!/bin/bash
skyplane deprovision; export SKYPLANE_DOCKER_IMAGE=$(bash scripts/pack_docker_broadcast.sh lynnliu030); pip install -e ".[aws,azure,gcp]";
python3 skyplane/broadcast/test/test_random_broadcast.py gcp:asia-southeast2-a gcp:australia-southeast1-a gcp:southamerica-east1-a gcp:europe-west4-a gcp:europe-west6-a gcp:asia-east1-a gcp:europe-west2-a -a Ndirect -c 1600 -n 2 -p 5 -gcp > log/network/gcp/Ndirect 
skyplane deprovision 
# python3 skyplane/broadcast/test/test_random_broadcast.py gcp:asia-southeast2-a gcp:australia-southeast1-a gcp:southamerica-east1-a gcp:europe-west4-a gcp:europe-west6-a gcp:asia-east1-a gcp:europe-west2-a -a ILP -c 800 -n 2 -p 5 -s 29 -gcp > log/network/gcp/ILP
# skyplane deprovision 
python3 skyplane/broadcast/test/test_random_broadcast.py gcp:asia-southeast2-a gcp:australia-southeast1-a gcp:southamerica-east1-a gcp:europe-west4-a gcp:europe-west6-a gcp:asia-east1-a gcp:europe-west2-a -a MDST -c 1600 -n 2 -p 5 -gcp > log/network/gcp/MDST
skyplane deprovision 
python3 skyplane/broadcast/test/test_random_broadcast.py gcp:asia-southeast2-a gcp:australia-southeast1-a gcp:southamerica-east1-a gcp:europe-west4-a gcp:europe-west6-a gcp:asia-east1-a gcp:europe-west2-a -a HST -c 1600 -n 2 -p 5 -gcp > log/network/gcp/HST