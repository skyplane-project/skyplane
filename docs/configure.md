# Configuration

Skyplane comes with a variety of knobs to tune to adjust performance or change how VMs are provisioned. 

You can configure these options using the `skyplane config set <key> <value>` command.

## CLI configuration
* `autoconfirm`: If set, it will not ask for you to confirm the transfers from the CLI. (default False)
* `autoshutdown_minutes`: If set, VMs will automatically shut down after this time in minutes. (default 15)
* `usage_stats`: If set, Skyplane will send aggregate performance statistics for a collective throughput grid. (default True)

## Transfer parallelism


* `max_instances`: Maximum number of instances to use for parallel transfers. (default 10)

Skyplane transfers files in parallel across a set of *gateway VMs*. The total bandwidth of a transfer is mostly determined by the number of VMs used. Skyplane will automatically partition and distribute the transfer across the set of gateway VMs.

By default, Skyplane will use a maximum of 1 VM in each region. This limit is conservative since too many VMs can potentially result failed transfers due to VCPU limits and other resource constraints. If you encounter VCPU limits, increase your VCPU limits following this [guide](increase_vcpus.md):


## Network configuration
* `bbr`: If set, the VM will use BBR congestion control instead of CUBIC. (default True)
* `compress`: If set, gateway VMs will compress data before egress to reduce costs. (default True)
* `encrypt_e2e`: If set, gateway VMs will encrypt data end-to-end. (default True)
* `encrypt_socket_tls`: If set, all sockets between gateways will be encrypted with TLS. (default False)
* `verify_checksums`: If set, gateway VMs will compute checksums at the source and verify checksums at the destination. (default True)
* `num_connections`: Number of connections to use between each gateway. (default 32)


## Object store configuration
* `multipart_enabled`: If set, multipart transfers will be enabled. (default False)
* `multipart_min_threshold_mb`: Minimum threshold in MB for multipart transfers. Below this, the object will be transferred in one chunk. (default 64).
* `multipart_min_size_mb`: Minimum size per chunk in MB for multipart transfers. (default 8).
* `multipart_max_chunks`: Maximum number of chunks for multipart transfers. (default 9990).
* `requester_pays`: If set, Skyplane will support requester pays buckets. (default False).


## Fallback to native commands
* `native_cmd_enabled`: If set, Skyplane will fallback to native commands if the transfer fails. (default True)
* `native_cmd_threshold_gb`: Transfers smaller than this threshold will be delegated to native commands. (default 2)

## Instance provisioning configuration
* `aws_instance_class`: AWS instance class to use for provisioning. (default m5.8xlarge)
* `aws_use_spot_instances`: If set, AWS will use spot instances instead of on-demand instances. (default False)
* `aws_default_region`: AWS region to use for provisioning. (default us-east-1)
* `azure_instance_class`: Azure instance class to use for provisioning. (default Standard_D32_v4)
* `azure_use_spot_instances`: If set, Azure will use spot instances instead of on-demand instances. (default False)
* `azure_default_region`: Azure region to use for provisioning. (default eastus)
* `gcp_instance_class`: GCP instance class to use for provisioning. (default n2-standard-32)
* `gcp_use_premium_network`: If set, will provision VMs on GCP's premium network tier. (default True)
* `gcp_service_account_name`: GCP service account name to use for provisioning. (default skyplane-manual)
* `gcp_use_spot_instances`: If set, GCP will use spot instances instead of on-demand instances. (default False)
* `gcp_default_region`: GCP region to use for provisioning. (default us-central1-a)
* `ibmcloud_use_spot_instances`: If set, IBM will use spot instances instead of on-demand instances. (default False) 
* `ibmcloud_default_region`: IBM region to use for provisioning. (default us-east) 
* `ibmcloud_instance_class`: IBM instance class to use for provisioning. (default bx2-2x8) 
* `scp_instance_class`: SCP instance class to use for provisioning. (default h1v32m128)
* `scp_default_region`: SCP region to use for provisioning. (default KR-WEST-1)

<!-- ### Transfer Chunk Sizes 
* Skyplane will break up large objects into smaller chunk sizes to parallelize transfers more efficiently (AWS and GCP only). 
* Recommended to use default (TODO: figure out what we should set this to)  -->

<!-- ## Reusing Gateways 
It can take 45s to 2m to provision gateway VMs for a transfer. If you are repeatedly transferring data between the same pair of regions, you can reuse gateway VMs. By default, Skyplane terminates these VMs to avoid unnecessary VM costs.

When running a `cp` or `sync` command, pass the `--reuse-gateways` flag to Skyplane to reuse gateway VMs:
```bash
$ skyplane cp --reuse-gateways <source> <destination>
$ skyplane sync --reuse-gateways <source> <destination>
```

We will attempt to automatically deprovision these gateways after 15 minutes by default. Change this interval via `skyplane config set autoshutdown_minutes <minutes>`. With `--reuse-gateways`, Skyplane will start a background job on each gateway VM that triggers a VM shutdown after the specified delay. Note you will still pay for associated VM costs such as storage and network IPs even if VMs are shut down.

To ensure that all gateways are stopped and no longer incur charges, run:
```bash
$ skyplane deprovision
``` --> 
