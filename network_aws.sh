#!/bin/bash
skyplane deprovision; export SKYPLANE_DOCKER_IMAGE=$(bash scripts/pack_docker_broadcast.sh lynnliu030); pip install -e ".[aws,gcp,azure]";
python3 skyplane/broadcast/test/test_random_broadcast.py aws:ap-east-1 aws:us-west-1 aws:ap-northeast-3 aws:eu-north-1 aws:ap-south-1 aws:ca-central-1 aws:ap-northeast-1 -a Ndirect -c 800 -n 2 -p 5 -aws > log/network/aws/Ndirect 
skyplane deprovision 
python3 skyplane/broadcast/test/test_random_broadcast.py aws:ap-east-1 aws:us-west-1 aws:ap-northeast-3 aws:eu-north-1 aws:ap-south-1 aws:ca-central-1 aws:ap-northeast-1 -a ILP -c 800 -n 2 -p 2 -s 40 -i -fe -aws > log/network/aws/ILP
skyplane deprovision 
python3 skyplane/broadcast/test/test_random_broadcast.py aws:ap-east-1 aws:us-west-1 aws:ap-northeast-3 aws:eu-north-1 aws:ap-south-1 aws:ca-central-1 aws:ap-northeast-1 -a MDST -c 800 -n 2 -p 5 -aws > log/network/aws/MDST
skyplane deprovision 