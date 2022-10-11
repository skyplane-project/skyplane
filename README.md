<picture>
    <source srcset="docs/_static/logo-dark-mode.png" media="(prefers-color-scheme: dark)">
    <img src="docs/_static/logo-light-mode.png" width="300" />
</picture>

[![Join Slack](https://img.shields.io/badge/-Join%20Skyplane%20Slack-blue?logo=slack)](https://join.slack.com/t/skyplaneworkspace/shared_invite/zt-1cxmedcuc-GwIXLGyHTyOYELq7KoOl6Q)
[![integration-test](https://github.com/skyplane-project/skyplane/actions/workflows/integration-test.yml/badge.svg)](https://github.com/skyplane-project/skyplane/actions/workflows/integration-test.yml)
[![docker](https://github.com/skyplane-project/skyplane/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/skyplane-project/skyplane/actions/workflows/docker-publish.yml)
[![docs](https://readthedocs.org/projects/skyplane/badge/?version=latest)](https://skyplane.readthedocs.io/en/latest/?badge=latest)

**🔥 Blazing fast bulk data transfers between any cloud 🔥**

Skyplane is a tool for blazingly fast bulk data transfers between object stores in the cloud. It provisions a fleet of VMs in the cloud to transfer data in parallel while using compression and bandwidth tiering to reduce cost.

Skyplane is:
1. 🔥 Blazing fast ([110x faster than AWS DataSync](https://skyplane.org/en/latest/benchmark.html))
2. 🤑 Cheap (4x cheaper than rsync)
3. 🌐 Universal (AWS, Azure and GCP)

You can use Skyplane to transfer data: 
* between object stores within a cloud provider (e.g. AWS us-east-1 to AWS us-west-2)
* between object stores across multiple cloud providers (e.g. AWS us-east-1 to GCP us-central1)
* between local storage and cloud object stores (experimental)

Skyplane supports all major public clouds including AWS, Azure, and GCP. It can also transfer data between any combination of these clouds:

<img src="docs/_static/supported-destinations.png" width="384" />

# Using Skyplane

The easiest way to use Skyplane is to use the CLI. `skyplane cp` supports any local path or cloud object store destination as an argument.

```bash
# copy files between two AWS S3 buckets
$ skyplane cp -r s3://... s3://...

# copy files from an AWS S3 bucket to a GCP GCS bucket
$ skyplane cp -r s3://... gs://...

# copy files from a local directory to/from a cloud object store
$ skyplane cp -r /path/to/local/files gs://...
```

Skyplane also supports incremental copies via `skyplane sync`:    
```bash
# copy changed files from S3 to GCS
$ skyplane sync s3://... gcs://...
```

`skyplane sync` will diff the contents of the source and destination and only copy the files that are different or have changed. It will not delete files that are no longer present in the source so it's always safe to run `skyplane sync`.

### Accelerating transfers with multiple VMs

With default arguments, Skyplane sets up a one VM (called gateway) in the source and destination regions. We can further accelerate the transfer by using more VMs.

To double the transfer speeds by using two VMs in each region, run:
```bash
$ skyplane cp -r s3://... s3://... -n 2
```

With 8 VMs per region, Skyplane is capable of moving data at up to 50Gbps.

If you do not have enough vCPU capacity in each region, you may get a InsufficientVCPUException. Either request more vCPUs or reduce the number of parallel VMs.

### ⚠️ Ensure the VMs that Skyplane starts are terminated
Skyplane will automatically attempt to terminate VMs that it starts, but to double check and forcefuly terminate all VMs, run `skyplane deprovision`.

# How Skyplane works
Skyplane is based on research at UC Berkeley into accelerated networks between cloud providers. Our NSDI 2023 paper is upcoming, but a [technical talk](https://skyplane.org/en/latest/architecture.html) is available.

<img src="docs/_static/skyplane-data-plane.png" width="384" />

Under the hood, Skyplane starts a fleet of VMs in the source and destination regions. It then uses a custom TCP protocol to accelerate the transfer between the VMs. Skyplane may use a L7 overlay network to route traffic around congested network hot spots. Notably, Skyplane does all this from a simple CLI interface.

# Getting started

## Installation
We recommend installation from PyPi:
```
$ pip install skyplane[aws]

# install support for other clouds as needed:
#   $ pip install skyplane[azure]
#   $ pip install skyplane[gcp]
#   $ pip install skyplane[all]
```

Skyplane supports AWS, Azure, and GCP. You can install Skyplane with support for one or more of these clouds by specifying the corresponding extras. To install two out of three clouds, you can run `pip install skyplane[aws,azure]`.

*GCP support on the M1 Mac*: If you are using an M1 Mac with the arm64 architecture and want to install GCP support for Skyplane, you will need to install as follows
`GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=1 GRPC_PYTHON_BUILD_SYSTEM_ZLIB=1 pip install skyplane[aws,gcp]`

## Authenticating with cloud providers

To transfer files from cloud A to cloud B, Skyplane will start VMs (called gateways) in both A and B. The CLI therefore requires authentication with each cloud provider. Skyplane will infer credentials from each cloud providers CLI. Therefore, log into each cloud.

### Setting up AWS credentials

To set up AWS credentials on your local machine, first [install the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).

After installing the AWS CLI, configure your AWS IAM access ID and secret with `aws configure` ([more details](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-quickstart.html#getting-started-quickstart-new)).
<!-- <details>
<summary>"aws configure" output</summary>
<br>
 
```bash
$ aws configure
AWS Access Key ID [None]: AKIAIOSFODNN7EXAMPLE
AWS Secret Access Key [None]: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
Default region name [None]: us-west-2
Default output format [None]: json
```
</details> -->

### Setting up GCP credentials
To set up GCP credentials on your local machine, first [install the gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk).

After installing the gcloud CLI, configure your GCP CLI credentials with `gcloud auth` as follows
```bash
$ gcloud auth login
$ gcloud auth application-default login
```
Ensure the GCP Compute Engine, Storage Engine, Cloud Resource Manager, and IAM APIs are enabled for the project.

### Setting up Azure credentials

To set up Azure credentials on your local machine, first [install the Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest).

After installing the Azure CLI, configure your Azure CLI credentials with `az login` as follows:
```bash
$ az login
```

Skyplane should now be able to authenticate with Azure although you may need to pass your subscription ID to `skyplane init` later.

## Importing cloud credentials into Skyplane

After authenticating with each cloud provider, you can run `skyplane init` to create a configuration file for Skyplane.

```bash
$ skyplane init
```
<details>
<summary>skyplane init output</summary>
<br>

```
$ skyplane init

====================================================
 _____ _   ____   _______ _       ___   _   _  _____
/  ___| | / /\ \ / / ___ \ |     / _ \ | \ | ||  ___|
\ `--.| |/ /  \ V /| |_/ / |    / /_\ \|  \| || |__
 `--. \    \   \ / |  __/| |    |  _  || . ` ||  __|
/\__/ / |\  \  | | | |   | |____| | | || |\  || |___
\____/\_| \_/  \_/ \_|   \_____/\_| |_/\_| \_/\____/
====================================================


(1) Configuring AWS:
    Loaded AWS credentials from the AWS CLI [IAM access key ID: ...XXXXXX]
    AWS region config file saved to /home/ubuntu/.skyplane/aws_config

(2) Configuring Azure:
    Azure credentials found in Azure CLI
    Azure credentials found, do you want to enable Azure support in Skyplane? [Y/n]: Y
    Enter the Azure subscription ID: [XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX]:
    Azure region config file saved to /home/ubuntu/.skyplane/azure_config
    Querying for SKU availbility in regions
    Azure SKU availability cached in /home/ubuntu/.skyplane/azure_sku_mapping

(3) Configuring GCP:
    GCP credentials found in GCP CLI
    GCP credentials found, do you want to enable GCP support in Skyplane? [Y/n]: Y
    Enter the GCP project ID [XXXXXXX]:
    GCP region config file saved to /home/ubuntu/.skyplane/gcp_config

Config file saved to /home/ubuntu/.skyplane/config
```

</details>
