# Increasing cloud vCPU limits

Skyplane utilizes parallel VMs to transfer data at high rates. However, if you do not have sufficient vCPU limits to support the number of VMs you need, you can increase the number of VMs you can use by requesting a quota increase from each respective cloud.

## Increasing AWS quotas
To increase your AWS quota, follow these steps:
1. Go to the [AWS EC2 console](https://console.aws.amazon.com/ec2/v2/home?region=us-east-1)
2. Select the region you want to increase your quota for from the top right corner of the page.
3. Select the **Quotas** tab.
4. Search for "Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances" and select the radio button on the left.
5. Click "Request quota increase".
6. In the form, request a higher vCPU allocation.
    * By default, each Skyplane VM uses 32 vCPUs and provides up to 5Gbps of network throughput on AWS.
    * Example: If you'd like to use 8 VMs per region, request 256 vCPUs.
7. Click "Request". AWS can take up to a few days to review the request.

## Increasing Azure quotas
To increase your Azure quota, follow these steps:
1. Go to the [Azure Quota request page](https://portal.azure.com/#blade/Microsoft_Azure_Capacity/QuotaMenuBlade/myQuotas) in the Azure portal.
2. Filter the quotas by your subscription:
    * Under Search, enter "Standard Dv4 Family vCPUs".
    * Under the service dropdown, select "Compute".
    * Under the subscription dropdown, select your Azure subscription.
    * Under region, select the regions you want to increase your quota for.
3. Checkmark all the quotas you want to increase.
4. Click "Request quota increase" and select "Enter a new limit".
5. Enter the number of vCPUs you want to increase your quota for.
    * By default, each Skyplane VM uses 32 vCPUs and provides up to 12.5Gbps on Azure.
    * Example: If you'd like to use 8 VMs per region, enter 256 vCPUs.
6. Click "Submit". Azure can take up to a few days to review the request.

## Increasing GCP quotas
To increase your GCP quotas, follow these steps:
1. Go to the [GCP Console](https://console.cloud.google.com/).
2. Select your project from the top dropdown.
3. Search for "All quotas" and navigate to the All quotas page.
4. Filter the quota list:
    * Under "Service", select "Compute Engine API".
    * Under "Quota", select "N2 CPUs"
5. Select all regions you want to increase your quota for.
6. Click "Edit quotas".
7. Increase the number of vCPUs you want to increase your quota for.
    * By default, each Skyplane VM uses 32 vCPUs and provides up to 7Gbps of network throughput on GCP.
    * Example: If you'd like to use 8 VMs per region, request 256 vCPUs. 
8. Click "Submit request". GCP can take up to a few days to review the request. In some cases, limit increases are approved immediately via automated review.
