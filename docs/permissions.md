# Required Permissions 
Skyplane runs in your own cloud accounts and does interact with an external service to run transfers. However the cloud account you are running Skyplane in must have permissions to interact with object storage and create VMs to execute transfers

## AWS  
Your AWS account must have the following permissions: 
* `Storage`
* `Multipart`
* `VM` 


## GCP  
Your GCP account must have the following permissions: 
* `CreateServiceAccount` 
* `ObjectStore`
* `VM` 

For GCP, Skyplane create a service account which has permissions to read and write from GCP object stores. 

TODO: check to see if removing obj store permissions from GCP service account still lets service credentials read. 

## Azure 
Your Azure account must have the following roles:
* `Storage Blob Data Contributor`
* `Storage Account Contributor`

NOTE: Within Azure, it is not sufficient to have just the `Owner` role to be able to access and write to containers in storage. The VMs that Skyplane provisions are assigned the sufficient storage permissions, but to be able to interact with Azure storage locally, check to make sure your personal Azure account has the roles listed above.
