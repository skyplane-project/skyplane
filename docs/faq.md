# FAQ

## I get `InsufficientVCPUException` when running a transfer

Skyplane needs to provision VMs in each cloud to complete a transfer. The VMs read data from the source object store and send it along sockets to the destination.
By default, Skyplane will provision 1 VM in each region where each VM has 32 vCPUs. If you do not have sufficient quota to provision these VMs, please see [our guide to requesting cloud limit increases](increase_vcpus).

Alternatively, you can [configure Skyplane](configure) to use fewer resources per transfer. You can reduce the default number of VMs used via `max_instances` and change the instance class for gateways by configuring `aws_instance_class`, `azure_instance_class` or `gcp_instance_class` with a smaller instance class.

## Does Skyplane support local transfers?

Skyplane does not currently support local transfers. Cloud to cloud transfers are supported.

## How does Skyplane map object keys from source to destination?

In the non-recursive case, Skyplane extracts the key from the full bucket path (s3://[bucket_name]/[key]) and places the object specified at dest_prefix/[key] if the dest_prefix provided is a directory (ends in `/`), or else replaces the object at dest_prefix with the extracted object.

In the recursive case, Skyplane appends a trailing slash to the source and dest paths if one does not already exist. After extracting the key from the source path, it is appended to the dest prefix to get object keys.

## I am unable to copy files to a folder in my destination bucket due to a Missing Object Error

Check to make sure your recursive flag is enabled.

<!-- ## Does Skyplane support VM-to-VM transfers? -->