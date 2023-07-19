***************
Required Permissions 
***************

Skyplane runs in your own cloud accounts and does interact with an external service to run transfers. However the cloud account you are running Skyplane in must have permissions to interact with object storage and create VMs to execute transfers.

AWS  
-----------------------------
Your AWS account must have the following permissions: 

- :code:`ec2:*`
- :code:`s3:*`
- :code:`kms:Decrypt`
- :code:`kms:GenerateDataKey`
- :code:`iam:GetRole`
- :code:`iam:CreateInstanceProfile`, 
- :code:`iam:AddRoleToInstanceProfile`

GCP  
-----------------------------
For GCP, Skyplane create a service account which has permissions to read and write from GCP object stores. Your GCP account must have the following roles: 

- :code:`roles/iam.serviceAccountCreator`
- :code:`roles/storage.objectAdmin`
- :code:`roles/compute.instanceAdmin.v1`

In addition, the *Compute Engine default service account* principal must have the :code:`roles/storage.objectAdmin` role, since instances created by the principal will inherit its permissions. 


Azure 
-----------------------------
Your Azure account must have the following roles:

- :code:`Storage Blob Data Contributor`
- :code:`Storage Account Contributor`


.. note::
    
   Within Azure, it is not sufficient to have just the :code:`Owner` role to be able to access and write to containers in storage. The VMs that Skyplane provisions are assigned the sufficient storage permissions, but to be able to interact with Azure storage locally, check to make sure your personal Azure account has the roles listed above.



