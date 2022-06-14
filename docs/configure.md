# Configuration

## Resource Limits
* Setting more aggressive VCPU limits 


## Transfer Options
You can configure transfer parameters (TODO: link to CLI documentation)

### Transfer Chunk Sizes 
* Skyplane will break up large objects into smaller chunk sizes to parallelize transfers more efficiently (AWS and GCP only). 
* Recommended to use default (TODO: figure out what we should set this to) 

### Reusing Gateways 
If you are running repeated transfers between the same source and destination, you can reduce the transfer time by using the `--reuse-gateways` flag, which will keep gateway instances running even after the transfer is completed. Make sure to run `skyplane deprovision` once you're done to prevent unnecessary charges.  

### Compression


