# Debugging Tools
Skyplane has built-in tools for debugging during development. 

## Logs 

### Client Logs
When you run a transfer, the client logs will be written to a folder inside of the `/tmp/skyplane/transfer_logs/` directory. 

### Gateway Logs
Inside the `client.log` file, for each provisioned Skyplane gateway there will be the lines: 
```
[INFO]     Log viewer: http://127.0.0.1:[PORT]/container/[CONTAINER_ID] 
[INFO]     API: http://127.0.0.1:[PORT]
```
which correspond to the gateway log viewer and the gateway chunk API. You can view the logs for each gateway by going to the "Log viewer" address for that gateway. 

### Chunk API 
The gateway chunk API allows for status of chunks on each gateway to be queried externally, and is used by the Skyplane client to monitor transfers. 
```
    * GET /api/v1/status - returns status of API
    * GET /api/v1/servers - returns list of running servers
    * GET /api/v1/chunk_requests - returns list of chunk requests (use {'state': '<state>'} to filter)
    * GET /api/v1/chunk_requests/<int:chunk_id> - returns chunk request
    * POST /api/v1/chunk_requests - adds a new chunk request
    * PUT /api/v1/chunk_requests/<int:chunk_id> - updates chunk request
    * GET /api/v1/chunk_status_log - returns list of chunk status log entries
```

## Gateway `ssh`
You can `ssh` into a gateway with: 
```
skyplane ssh
```
which will list available gateways that you can select to ssh into. 

Once you've ssh-ed into a gateway instance, you can interact with the Skyplane docker image (??).