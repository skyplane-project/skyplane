Instructions for data transfer. 
===
server:  3.237.190.187 this should be added to gfg-server.cpp#12
client:  44.197.201.109
g++ -fpermissive gfg-server.cpp -o gfg-server && ./gfg-server
g++ -fpermissive gfg-client.cpp -o gfg-client && ./gfg-client

sources: [stackoverflow](https://stackoverflow.com/questions/26072451/multiple-socket-connections-for-file-transfer), [gfg](www.geeksforgeeks.org/c-program-for-file-transfer-using-udp/)

hiredis: 
`wget https://github.com/redis/hiredis/releases/tag/v0.14.0
tar -xvzf v0.14.0.tar.gz
cd hiredis-0.14.0/
make
sudo make install
gcc redistest.c -o redistest -I /usr/local/include/hiredis -g -lhiredis && ./redistest 
`

`g++ main.cpp skylark.cpp -o test`


boost
`
wget https://boostorg.jfrog.io/artifactory/main/release/1.67.0/source/boost_1_67_0.tar.bz
2
tar --bzip2 -xf boost_1_67_0.tar.bz2
cd boost_1_67_0/
./bootstrap.sh
./b2 
sudo ./b2 install
g++ -DBOOST_LOG_DYN_LINK logging_test.cpp main.cpp skylark.cpp -lboost_log -lpthread -o test
`
