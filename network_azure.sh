#!/bin/bash
skyplane deprovision; export SKYPLANE_DOCKER_IMAGE=$(bash scripts/pack_docker_broadcast.sh lynnliu030); pip install -e ".[aws,gcp,azure]";
python3 skyplane/broadcast/test/test_random_broadcast.py azure:brazilsouth azure:westeurope azure:westus azure:koreacentral azure:australiaeast azure:uaenorth azure:centralindia -a Ndirect -c 800 -n 2 -p 5 -azure > log/network/azure/Ndirect 
skyplane deprovision 
python3 skyplane/broadcast/test/test_random_broadcast.py azure:brazilsouth azure:westeurope azure:westus azure:koreacentral azure:australiaeast azure:uaenorth azure:centralindia -a ILP -c 800 -n 2 -p 5 -s 29 -azure > log/network/azure/ILP
skyplane deprovision 
python3 skyplane/broadcast/test/test_random_broadcast.py azure:brazilsouth azure:westeurope azure:westus azure:koreacentral azure:australiaeast azure:uaenorth azure:centralindia -a MDST -c 800 -n 2 -p 5 -azure > log/network/azure/MDST
skyplane deprovision 
python3 skyplane/broadcast/test/test_random_broadcast.py azure:brazilsouth azure:westeurope azure:westus azure:koreacentral azure:australiaeast azure:uaenorth azure:centralindia -a HST -c 800 -n 2 -p 5 -azure > log/network/azure/HST