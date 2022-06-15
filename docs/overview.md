# Achitecture
Skyplane performs high-performance, cost-efficient, bulk data transfers by parallelizing transfers, provisioning resources for transfers, and identifying optimal transfer paths. Skyplane profiles cloud network cost and throughput across regions, and borrows ideas from [RON](http://nms.csail.mit.edu/ron/) to identify optimal transfer paths across regions and cloud providers. 

## Components
TODO (insert diagram)

Terminology: 
* Client: Runs on a user's local machine, and managed resource provisioning and planning for transfers. 
* Gateway: Additional instances spun up by the client responsible for interfacing with object stores and relaying chunks to other gateways.
* Chunk: Unit of data (either a single file or part of file) being transferred. 
* Overlay Path: A path consisting of gateways and gateway links to optimize for a transfer. 


## Transfer Integrity
TODO @ Paras

## Security 
TODO @ Sam

### Encryption 

### Firewalls

## Large Objects
Skyplane breaks large objects into smaller sub-parts (currently AWS and GCP only) to improve transfer parallelism (also known as [striping](https://ieeexplore.ieee.org/document/1560006)). 