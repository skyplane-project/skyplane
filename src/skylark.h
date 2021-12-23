#ifndef __SKYLARK__
#define __SKYLARK__

#include <iostream>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <hiredis/hiredis.h>


// For all things skylark
namespace skylark{
  // To identify and check for existance of files
  class FileSys{
      int8_t errorCode;
      redisReply *reply;
      redisContext *c;
      char *errorFilePath = "NULL";
    public:
      FileSys(const char* redis_ip, int redis_port);
      ~FileSys();
      bool doesFileExist(const char* fileName);
      char *getFilePath(const char* fileName);
      int8_t getErrorCode();
  }; 
  
  
  
  // To query and register skylark gateways in different AZs and operators
  class Gateways{
    public:
      Gateways();
  }; 
  
  
  
  // For control-plane hop determination.
  class NextHop{
    public:
      NextHop();
  }; 
  // Technically for us as we keep sockets open between any 2 gateways, 
  // a {file, gateway, next-hop} tuple uniquely represents each 'flow'.
}

#endif //__SKYARK__
