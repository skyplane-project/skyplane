# Configuration

Skyplane comes with a variety of knobs to tune to adjust performance or change how VMs are provisioned. You can configure these options using the `skyplane config set <key> <value>` command.

```{admonition} Full list of transfer options
* CLI configuration
    * `autoconfirm`: If set, it will not ask for you to confirm the transfers from the CLI. (default False)
    * `autoshutdown_minutes`: If set, VMs will automatically shut down after this time in minutes. (default 15)
* Transfer parallelism
    * `max_instances`: Maximum number of instances to use for parallel transfers. (default 10)
* Network configuration
    * `compress`: If set, gateway VMs will compress data before egress to reduce costs. (default True)
    * `encrypt_e2e`: If set, gateway VMs will encrypt data end-to-end. (default True)
    * `encrypt_socket_tls`: If set, all sockets between gateways will be encrypted with TLS. (default False)
    * `verify_checksums`: If set, gateway VMs will compute checksums at the source and verify checksums at the destination. (default True)
    * `num_connections`: Number of connections to use between each gateway. (default 32)
* Multipart transfer configuration
    * `multipart_enabled`: If set, multipart transfers will be enabled. (default False)
    * `multipart_max_chunk_size_mb`: Maximum chunk size in MB for multipart transfers. (default 8).
* Instance provisioning configuration
    * `aws_instance_class`: AWS instance class to use for provisioning. (default m5.8xlarge)
    * `azure_instance_class`: Azure instance class to use for provisioning. (default Standard_D32_v4)
    * `gcp_instance_class`: GCP instance class to use for provisioning. (default n2-standard-32)
    * `gcp_use_premium_network`: If set, will provision VMs on GCP's premium network tier. (default True)
```

## Increasing performance of transfers via paralllelism
Skyplane transfers files in parallel across a set of *gateway VMs*. The total bandwidth of a transfer is mostly determined by the number of VMs used. Skyplane will automatically partition and distribute the transfer across the set of gateway VMs.

To increase the parallelism of a transfer, run:
```bash
$ skyplane config set max_instances <number>
```

By default, Skyplane will use a maximum of 1 VM in each region. This limit is conservative since too many VMs can potentially result failed transfers due to VCPU limits and other resource constraints.

If you encounter VCPU limits, increase your VCPU limits following this guide:
```{toctree}

increase_vcpus
```

<!-- ### Transfer Chunk Sizes 
* Skyplane will break up large objects into smaller chunk sizes to parallelize transfers more efficiently (AWS and GCP only). 
* Recommended to use default (TODO: figure out what we should set this to)  -->

## Reusing Gateways 
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
```

## Configuring networking between gateways
Skyplane supports encrypting data end-to-end. This is useful for encrypting data that is stored on a local disk. We enable end-to-end encryption by default. To disable it, run:
```bash
$ skyplane config set encrypt_e2e false
```

Skyplane automatically compresses data at the source region to reduce egress fees from data transfer. We use the [LZ4](https://lz4.org/) compression algorithm by default as it can compress data at line rate. To disable compression, run:
```bash
$ skyplane config set compress false
```

Skyplane continually computes checksums at the source region to verify data integrity. We use the [MD5](https://en.wikipedia.org/wiki/MD5) checksum algorithm by default. To disable checksum verification, run:
```bash
$ skyplane config set verify_checksums false
```

Optionally and in addition to end-to-end encryption, Skyplane supports sending data over a TLS encrypted socket. We don't generally recommend using this unless you have a specific security requirement as it can increase performance variablity of transfers. To enable further TLS socket encryption, run:
```bash
$ skyplane config set encrypt_socket_tls true
```