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

## Troubleshooting MissingObjectException
This exception is raised when:
* no objects are found at the source prefix passed to the CLI
* the source prefix is a directory and the `--recursive` flag is not set
* there is a mismatch between the source prefix and the key for an object Skyplane is copying

To troubleshoot this exception, carefully check that the requested path is not empty and is accessible via the credential used by Skyplane. If this is the case, ensure that the `--recursive` flag is set if the source prefix is a directory.

As an example, to transfer the directory `s3://some-bucket/some-directory/` to `s3://some-bucket/destination/`, you would run `skyplane cp --recursive s3://some-bucket/some-directory/ s3://some-bucket/destination/`.

## Troubleshooting MissingBucketException
This exception is raised when:
* the source bucket does not exist
* the destination bucket does not exist
* the source bucket is not accessible via the credential used by Skyplane
* the destination bucket is not accessible via the credential used by Skyplane

Using the cloud provider's console, verify the bucket exists. If so, ensure that Skyplane has access to the bucket.

```{note}
**Requester pays buckets**: If it is a public bucket, it may be a [requester pays bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/RequesterPaysBuckets.html). Any egress fees from this bucket will be paid by the requester (i.e. Skyplane) instead of the bucket's owner. By default Skyplane disables support for requester pays to avoid unexpected egress charges.

To enable support for requester pays buckets, run `skyplane config set requester_pays true`.
``` 

# How can I switch between GCP projects? 
We recommend re-setting GCP credentials locally by running `rm -r ~/.config/gcloud` then re-running `gcloud auth application-default login`. You can then set the project ID you want with `gcloud config set project <PROJECT_ID>`. Once you've updated authentication and the project, you can run `skyplane init --reinit-gcp'. 
 
If you get a an error saying `Compute Engine API has not been used in project 507282715251 before or it is disabled`, wait a few minutes for the API enablement to take effect and re-run `skyplane init`.
