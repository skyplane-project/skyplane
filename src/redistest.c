#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <hiredis/hiredis.h>

int main(){
	redisReply *reply; 	
	redisContext *c = redisConnect("127.0.0.1", 6379); 
	if (c != NULL && c->err) { 
		printf("Error: %sn", c->errstr); // handle error 
	} else { 
		printf("Connected to Redis \n"); 
	} 
  reply = redisCommand(c,"PING %s", "Hello World");
  printf("RESPONSE: %s\n", reply->str);
  freeReplyObject(reply);
  reply = redisCommand(c,"SET %s %s","foo","bar"); 
  freeReplyObject(reply); 
  reply = redisCommand(c, "EXISTS %s", "foo");
  printf("%lld \n", reply->integer);
  freeReplyObject(reply);
  reply = redisCommand(c,"GET %s", "foo"); 
  printf("%s \n",reply->str); 
  freeReplyObject(reply);
	redisFree(c);
}
