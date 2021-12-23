#include "skylark.h"

skylark::FileSys::FileSys(const char* redis_ip, int redis_port){
	this->errorCode = 0;
  this->c = redisConnect(redis_ip, redis_port); 
	if (this->c != NULL && this->c->err) { 
    printf("Error: %sn", c->errstr); // handle error 
    this->errorCode = 1; // Failed to connect to Redis
	} else { 
    printf("Connected to Redis \n"); 
	} 
  std::cout<<"FileSys initialized"<<std::endl;
}

skylark::FileSys::~FileSys(){
  freeReplyObject(this->reply);
  redisFree(this->c);
}

int8_t skylark::FileSys::getErrorCode(){
  return this->errorCode;
}

bool skylark::FileSys::doesFileExist(const char* fileName){
  freeReplyObject(this->reply);
  reply =(redisReply *) redisCommand(this->c, "EXISTS %s", fileName);
  if (reply->integer==1) return true;
  else return false;
}

char *skylark::FileSys::getFilePath(const char* fileName){
  if (doesFileExist(fileName)==false)
    return this->errorFilePath;
  freeReplyObject(this->reply);
  reply =(redisReply *) redisCommand(this->c, "GET %s", fileName);
  return reply->str;
}


skylark::Gateways::Gateways(){
  std::cout<<"Gateways initialized"<<std::endl;
}

skylark::NextHop::NextHop(){
  std::cout<<"NextHop initialized"<<std::endl;
}
