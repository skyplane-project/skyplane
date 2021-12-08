#ifndef __SKYLARK__
#define __SKYLARK__

#include <iostream>

// For all things skylark
namespace skylark{
  // To identify and check for existance of files
  class FileSys{
    public:
      FileSys();
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
