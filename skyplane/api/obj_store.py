from skyplane import compute
from typing import Optional
from skyplane.obj_store.object_store_interface import ObjectStoreInterface


class ObjectStore:

    def __init__(
        self,
        aws_auth: Optional[compute.AWSAuthentication] = None,
        azure_auth: Optional[compute.AzureAuthentication] = None,
        gcp_auth: Optional[compute.GCPAuthentication] = None,
        host_uuid: Optional[str] = None,
        ibmcloud_auth: Optional[compute.IBMCloudAuthentication] = None,
    ):
        """
        :param aws_auth: authentication information for aws
        :type aws_auth: compute.AWSAuthentication
        :param azure_auth: authentication information for azure
        :type azure_auth: compute.AzureAuthentication
        :param gcp_auth: authentication information for gcp
        :type gcp_auth: compute.GCPAuthentication
        :param host_uuid: the uuid of the local host that requests the provision task
        :type host_uuid: string
        :param ibmcloud_auth: authentication information for aws
        :type ibmcloud_auth: compute.IBMCloudAuthentication
        """
        self.aws_auth = aws_auth
        self.azure_auth = azure_auth
        self.gcp_auth = gcp_auth
        self.host_uuid = host_uuid
        self.ibmcloud_auth = ibmcloud_auth

    def create_interface(self, provider, bucket_name):
        return ObjectStoreInterface.create(
            f"{provider}:infer", 
            bucket_name,
            self.aws_auth,
            self.azure_auth,
            self.gcp_auth,
            self.host_uuid,
            self.ibmcloud_auth
        )

    def download_object(self, bucket_name: str, provider: str, key: str, filename: str):
        obj_store = self.create_interface(provider, bucket_name)
        obj_store.download_object(key, filename)

    def upload_object(self, filename: str, bucket_name: str, provider: str, key: str):
        obj_store = self.create_interface(provider, bucket_name)
        obj_store.upload_object(filename, key)

    def exists(self, bucket_name: str, provider: str, key: str) -> bool:
        obj_store = self.create_interface(provider, bucket_name)
        return obj_store.exists(key)

    def bucket_exists(self, bucket_name: str, provider: str) -> bool:
        # azure not implemented
        if provider == "azure":
            raise NotImplementedError(f"Provider {provider} not implemented")

        obj_store = self.create_interface(provider, bucket_name)
        return obj_store.bucket_exists()

    def create_bucket(self, region: str, bucket_name: str):
        provider = region.split(":")[0]
        # azure not implemented
        if provider == "azure":
            raise NotImplementedError(f"Provider {provider} not implemented")

        obj_store = self.create_interface(provider, bucket_name)
        obj_store.create_bucket(region.split(":")[1])

        # TODO: create util function for this
        if provider == "aws":
            return f"s3://{bucket_name}"
        elif provider == "gcp":
            return f"gs://{bucket_name}"
        else:
            raise NotImplementedError(f"Provider {provider} not implemented")

    def delete_bucket(self, bucket_name: str, provider: str):
        # azure not implemented
        if provider == "azure":
            raise NotImplementedError(f"Provider {provider} not implemented")

        obj_store = self.create_interface(provider, bucket_name)
        obj_store.delete_bucket()
