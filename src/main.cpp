#include <iostream>
//#include <boost/log/trivial.hpp>
#include "skylark.h"

int main(){
  // Tests
  skylark::FileSys filesys_obj("127.0.0.1", 6379);
  skylark::Gateways gateway_obj;
  skylark::NextHop nexthop_obj;
  return 0;
}
