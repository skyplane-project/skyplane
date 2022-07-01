# FAQ

## I get `InsufficientVCPUException` when running a transfer

Skyplane needs to provision VMs in each cloud to complete a transfer. The VMs read data from the source object store and send it along sockets to the destination.
By default, Skyplane will provision 1 VM in each region where each VM has 32 vCPUs. If you do not have sufficient quota to provision these VMs, please see [our guide to requesting cloud limit increases](increase_vcpus).

Alternatively, you can [configure Skyplane](configure) to use fewer resources per transfer. You can reduce the default number of VMs used via `max_instances` and change the instance class for gateways by configuring `aws_instance_class`, `azure_instance_class` or `gcp_instance_class` with a smaller instance class.

<!-- ## How does Skyplane map object keys from source to destination? -->

<!-- ## Does Skyplane support VM-to-VM transfers? -->