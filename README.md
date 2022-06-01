# Skyplane: A Unified Data Layer for the Multi-Cloud


[![Docker](https://github.com/skyplane-project/skyplane/actions/workflows/docker-publish.yml/badge.svg?branch=main)](https://github.com/skyplane-project/skyplane/actions/workflows/docker-publish.yml)

# Introduction
Skyplane is a tool for blazingly fast bulk data transfers in the cloud. Skyplane manages parallelism, data partitioning, and network paths to optimize data transfers, and can also spin up VM instances to increase transfer throughput. 

You can use skyplane to transfer data: 
* Between buckets within a cloud provider
* Between object stores across multiple cloud providers
* To/from local storage to a cloud object store

# Getting started

## Installation

We recommend installation from PyPi: `pip install skyplane-nightly`

From source:
```bash
$ git clone https://github.com/skyplane-project/skyplane
$ cd skyplane
$ pip install -e .
```

## Authenticating with cloud providers

To transfer files from cloud A to cloud B, Skyplane will start VMs (called gateways) in both A and B. The CLI therefore requires authentication with each cloud provider. Skyplane will infer credentials from each cloud providers CLI. Therefore, log into each cloud.

<details>
<summary>Setting up AWS credentials</summary>
<br>

To set up AWS credentials on your local machine, first [install the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).

After installing the AWS CLI, configure your AWS IAM access ID and secret with `aws configure`:
```bash
$ aws configure
AWS Access Key ID [None]: AKIAIOSFODNN7EXAMPLE
AWS Secret Access Key [None]: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
Default region name [None]: us-west-2
Default output format [None]: json
```

See AWS documentation for further [instructions on how to configure the AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-quickstart.html#getting-started-quickstart-new).

</details>

<details>
<summary>Setting up GCP credentials</summary>
<br>

To set up GCP credentials on your local machine, first [install the gcloud CLI](https://cloud.google.com/sdk/docs/install-sdk).

After installing the gcloud CLI, configure your GCP CLI credentials with `gcloud auth` as follows:
```bash
$ gcloud auth login
$ gcloud auth application-default login
```

If you already had GCP credentials configured, make sure to run `gcloud auth application-default login` which generates application credentials for Skyplane.
</details>

<details>
<summary>Setting up Azure credentials</summary>
<br>

To set up Azure credentials on your local machine, first [install the Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest).

After installing the Azure CLI, configure your Azure CLI credentials with `az login` as follows:
```bash
$ az login
```

Skyplane should now be able to authenticate with Azure although you may need to pass your subscription ID to `skyplane init` later.
</details>

### Importing cloud credentials into Skyplane

After authenticating with each cloud provider, you can run `skyplane init` to create a configuration file for Skyplane.

```bash
$ skyplane init
```
<details>
<summary>skyplane init output</summary>
<br>

```bash
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