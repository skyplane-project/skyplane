# Architecture
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

## Encryption 

## Firewalls

Skyplane adopts best practices to ensure data and gateway nodes are secure during transfers. In this section, we describe the design in-brief. Firewalls are enabled by default, and we advise you not to turn them off. This ensures not only is the data secure in flight, but also prevents gateways from being compromised.  Our approach of having unique `skyplane` VPC and firewalls  guarantees that your default networks remain untouched, and we have also architected it to allow for multiple simultaneous transfers! If you have any questions regarding the design and/or implementation we encourage you to open an issue with `[Firewall]` in the title. 

### GCP
Skyplane creates a global VPC called [`skyplane`](https://github.com/skyplane-project/skyplane/blob/e5c97e007b69673558ade0396df490a98227dcc0/skyplane/compute/gcp/gcp_cloud_provider.py#L154) when it is invoked for the first time with a new subscription-id. Instances and firewall rules are applied on this VPC and do NOT interfere with the `default` GCP VPC. This ensures all the changes that Skyplane introduces are localized within the skyplane VPC - all instances and our firewalls rules only apply within the `skyplane` VPC. The `skyplane` global VPC consists of `skyplane` sub-nets for each region. 

During every `skyplane` transfer, a new set of firewalls are [created](https://github.com/skyplane-project/skyplane/blob/e5c97e007b69673558ade0396df490a98227dcc0/skyplane/compute/gcp/gcp_cloud_provider.py#L218) that allow IPs of all instances that are involved in the transfer to exchange data with each other. These firewalls are set with priority 1000, and are revoked after the transfer completes. All instances can be accessed via `ssh` on port 22, and respond to `ICMP` packets to aid debugging. 

### AWS

While GCP VPCs are Global, in AWS for every region that is involved in a transfer, Skyplane creates a `skyplane` [VPC](https://github.com/skyplane-project/skyplane/blob/e5c97e007b69673558ade0396df490a98227dcc0/skyplane/compute/aws/aws_cloud_provider.py#L93), and a [security group](https://github.com/skyplane-project/skyplane/blob/e5c97e007b69673558ade0396df490a98227dcc0/skyplane/compute/aws/aws_cloud_provider.py#L153) (SG). During transfers, firewall rules are [instantiated](https://github.com/skyplane-project/skyplane/blob/e5c97e007b69673558ade0396df490a98227dcc0/skyplane/compute/aws/aws_cloud_provider.py#L267) that allow all IPs of gateway instances involved in the transfer to relay data with each other. Post the transfer, the firewalls are [deleted](https://github.com/skyplane-project/skyplane/blob/e5c97e007b69673558ade0396df490a98227dcc0/skyplane/compute/aws/aws_cloud_provider.py#L283).  

### Azure

Firewall support for Azure is in the roadmap. 

## Large Objects
Skyplane breaks large objects into smaller sub-parts (currently AWS and GCP only) to improve transfer parallelism (also known as [striping](https://ieeexplore.ieee.org/document/1560006)). 