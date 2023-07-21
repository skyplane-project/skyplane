<p align="center">
    <picture>
        <source srcset="docs/_static/logo-dark-mode.png" media="(prefers-color-scheme: dark)">
        <img src="docs/_static/logo-light-mode.png" width="300" />
    </picture>
</p>

<p align="center">
  <a href="https://join.slack.com/t/skyplaneworkspace/shared_invite/zt-1cxmedcuc-GwIXLGyHTyOYELq7KoOl6Q"> 
    <img alt="Join Slack" src="https://img.shields.io/badge/-Join%20Skyplane%20Slack-blue?logo=slack">
  </a>
  
  <a href="https://github.com/skyplane-project/skyplane/actions/workflows/integration-test-multiple-sizes.yml"> 
    <img alt="integration-test" src="https://github.com/skyplane-project/skyplane/actions/workflows/integration-test-multiple-sizes.yml/badge.svg">
  </a>
  
  <a href="https://github.com/skyplane-project/skyplane/actions/workflows/docker-publish.yml"> 
    <img alt="docker" src="https://github.com/skyplane-project/skyplane/actions/workflows/docker-publish.yml/badge.svg">
  </a>
  
  <a href="https://skyplane.readthedocs.io/en/latest/?badge=latest"> 
    <img alt="docs" src="https://readthedocs.org/projects/skyplane/badge/?version=latest">
  </a>
</p>

**🔥 Blazing fast bulk data transfers between any cloud 🔥**

Skyplane is a tool for blazingly fast bulk data transfers between object stores in the cloud. It provisions a fleet of VMs in the cloud to transfer data in parallel while using compression and bandwidth tiering to reduce cost.

Skyplane is:
1. 🔥 Blazing fast ([110x faster than AWS DataSync](https://skyplane.org/en/latest/benchmark.html))
2. 🤑 Cheap (4x cheaper than rsync)
3. 🌐 Universal (AWS, Azure, IBM and GCP)

You can use Skyplane to transfer data: 
* between object stores within a cloud provider (e.g. AWS us-east-1 to AWS us-west-2)
* between object stores across multiple cloud providers (e.g. AWS us-east-1 to GCP us-central1)
* between local storage and cloud object stores (experimental)

Skyplane currently supports the following source and destination endpoints (any source and destination can be combined): 

| Endpoint           | Source             | Destination        |
|--------------------|--------------------|--------------------|
| AWS S3             | :white_check_mark: | :white_check_mark: |
| Google Storage     | :white_check_mark: | :white_check_mark: |
| Azure Blob Storage | :white_check_mark: | :white_check_mark: |
| IBM Cloud Object Storage | :white_check_mark: | :white_check_mark: |
| Local Disk         | :white_check_mark: | (in progress)      |

Skyplane is an actively developed project. It will have 🔪 SHARP EDGES 🔪. Please file an issue or ask the contributors via [the #help channel on our Slack](https://join.slack.com/t/skyplaneworkspace/shared_invite/zt-1cxmedcuc-GwIXLGyHTyOYELq7KoOl6Q) if you encounter bugs.

# Resources 
- [Quickstart](#quickstart)
- [Contributing](https://skyplane.org/en/latest/contributing.html)
- [Roadmap](https://skyplane.org/en/latest/roadmap.html)
- [Slack Community](https://join.slack.com/t/skyplaneworkspace/shared_invite/zt-1cxmedcuc-GwIXLGyHTyOYELq7KoOl6Q)

# Quickstart

## 1. Installation
We recommend installation from PyPi:
```
$ pip install "skyplane[aws]"

# install support for other clouds as needed:

#   $ pip install "skyplane[azure]"
#   $ pip install "skyplane[gcp]"
#   $ pip install "skyplane[ibmcloud]"
#   $ pip install "skyplane[all]"
```

Skyplane supports AWS, Azure, IBM and GCP. You can install Skyplane with support for one or more of these clouds by specifying the corresponding extras. To install two out of three clouds, you can run `pip install "skyplane[aws,azure]"`.

*GCP support on the M1 Mac*: If you are using an M1 Mac with the arm64 architecture and want to install GCP support for Skyplane, you will need to install as follows
`GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=1 GRPC_PYTHON_BUILD_SYSTEM_ZLIB=1 pip install "skyplane[aws,gcp]"`

## 2. Setup Cloud Credentials 

Skyplane needs access to cloud credentials to perform transfers. To get started with setting up credentials, make sure you have cloud provider CLI tools installed:

```
---> For AWS:
$ pip install awscli

---> For Google Cloud:
$ pip install gcloud

---> For Azure:
$ pip install azure

```
Once you have the CLI tools setup, log into each cloud provider's CLI:
```
---> For AWS:
$ aws configure

---> For Google Cloud:
$ gcloud auth application-default login

---> For Azure:
$ az login

---> For IBM Cloud:
$ Follow IBM Cloud and create an account with the resource group.
Copy https://github.com/skyplane-project/skyplane/blob/main/skyplane/compute/ibmcloud/ibm_credentials.yaml.template
into `~/.bluemix/ibm_credentials` and fill your 
IBM IAM key and credentials to your IBM Cloud object storage 


```
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

## 3. Run Transfers 

We’re ready to use Skyplane! Let’s use `skyplane cp` to copy files from AWS to GCP:
```
skyplane cp s3://... gs://...
```
To transfer only new objects, you can instead use `skyplane sync`:
```
$ skyplane sync s3://... gs://...
```

You can configure Skyplane to use more VMs per region with the `-n` flag. For example, to double the transfer speed with two VMs, run: 
```
$ skyplane cp -r s3://... s3://... -n 2
```

## 4. Clean Up 
Skyplane will automatically attempt to terminate VMs that it starts, but to double check and forcefuly terminate all VMs, run `skyplane deprovision`.

# Technical Details
Skyplane is based on research at UC Berkeley into accelerated networks between cloud providers. Under the hood, Skyplane starts a fleet of VMs in the source and destination regions. It then uses a custom TCP protocol to accelerate the transfer between the VMs. Skyplane may use a L7 overlay network to route traffic around congested network hot spots. 

<img src="docs/_static/skyplane-data-plane.png" width="384" />

For more details on Skyplane, see: 
- [Technical Talk](https://skyplane.org/en/latest/architecture.html)
- [NSDI '23 Paper](https://arxiv.org/abs/2210.07259)


