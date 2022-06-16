# Debugging Tools
Skyplane has built-in tools for debugging during development.


## Gateway Logs 

### Dozzle
You can view gateway logs at ???.

### Gateway `ssh` 
You can `ssh` into a gateway with: 
```
skyplane ssh
```
which will list running gateways you can `ssh` into. 

To view the gateway logs on the instance, you can view the docker logs with: 
```
docker logs ??
```
You can view downloaded chunks in the ?? folder.


## Chunk API 
For a running gateway, you can use HTTP requests to view the chunk state on that gateway. 

TODO: can we auto-document the chunk API? 