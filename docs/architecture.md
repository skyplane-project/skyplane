# Architecture
Skyplane performs high-performance, cost-efficient, bulk data transfers by parallelizing transfers, provisioning resources for transfers, and identifying optimal transfer paths. Skyplane profiles cloud network cost and throughput across regions, and borrows ideas from [RON](http://nms.csail.mit.edu/ron/) to identify optimal transfer paths across regions and cloud providers. 

To learn about how Skyplane works, please see our talk here:
<iframe width="560" height="315" src="https://www.youtube-nocookie.com/embed/hOCrpcIBkAU" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>

## On prem support
Skyplane now supports local to cloud object store transfers. For this, Skyplane defaults to awscli (for AWS), and gsutil (for GCP). [Let us know](https://github.com/skyplane-project/skyplane/issues/545) if you would like to see on-prem support for Azure.

## Transfer Integrity and Checksumming
Skyplane takes several steps to ensure the correctness of transfers. To ensure that data is transferred without corruption (e.g. bit flips or missing byte ranges), Skyplane will compute checksums for data at the source region and verify data matches the checksum before writing back to the destination region. To ensure that no files are dropped during the transfer, Skyplane will query the destination object store after a transfer and check all files were copied with the correct file size. To verify checksums for whole-file transfers, Skyplane computes MD5 hashes at the source region. Upon writing data at the destination, hashes are validated directly in the destintation object store. For multipart transfers, hashses are validated at the destination VM before writing to the object store.

## Security
Data transfers in Skyplane are encrypted end-to-end. This means that all of the data chunks in each transfer are encrypted in the source region, transferred over the network (including through any relay regions) in encrypted form, and decrypted only when they reach destination region. Within the source and destination regions data may be handled in plaintext. For example, chunks are decrypted at the destination gateways and are inserted into the destination object store. For stronger security, the application using Skyplane may choose to store data in the source object store in encrypted form, so that it remains encrypted even in the source and destination regions. To afford higher efficiency for these use cases, Skyplane allows its own encryption to be disabled, to avoid encrypting data twice. The keys used for Skyplane's end-to-end encryption are generated at the client and then communicated to the gateways over SSH.

HTTP/REST calls made between gateways are enrypted separately, using TLS.

Owing to the above encryption mechanisms, Skyplane guarantees confidentiality against a passive adversary who can view data transferred over the wide-area network and in relay regions. Such an adversary cannot see the contents of the data, but it can potentially see the following:
* The quantity of data transferred.
* The network path and overlay path taken by each chunk during the transfer.
* The size of each chunk (which may be related to the size of the files/objects being transferred).
* The timing of each chunk's transfer between gateways and over the network.

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
