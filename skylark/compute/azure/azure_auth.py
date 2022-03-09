from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.authorization import AuthorizationManagementClient

class AzureAuthentication:
    def __init__(self, subscription_id: str):
        self.subscription_id = subscription_id
        self.credential = DefaultAzureCredential()
    
    def get_token(self, resource: str):
        return self.credential.get_token(resource)
    
    def get_compute_client(self):
        return ComputeManagementClient(self.credential, self.subscription_id)
    
    def get_resource_client(self):
        return ResourceManagementClient(self.credential, self.subscription_id)
    
    def get_network_client(self):
        return NetworkManagementClient(self.credential, self.subscription_id)
    
    def get_authorization_client(self):
        return AuthorizationManagementClient(self.credential, self.subscription_id)