from skyplane.utils import logger
from typing import Iterator, Any


class StorageInterface:
    def bucket(self) -> str:
        return self.bucket_name

    @property
    def provider(self) -> str:
        raise NotImplementedError()

    def region_tag(self) -> str:
        raise NotImplementedError()

    def path(self) -> str:
        raise NotImplementedError()

    def bucket(self) -> str:
        raise NotImplementedError()

    def create_bucket(self, region_tag: str):
        raise NotImplementedError()

    def delete_bucket(self):
        raise NotImplementedError()

    def bucket_exists(self) -> bool:
        raise NotImplementedError()

    def exists(self, obj_name: str) -> bool:
        raise NotImplementedError()

    def list_objects(self, prefix="") -> Iterator[Any]:
        raise NotImplementedError()

    @staticmethod
    def create(region_tag: str, bucket: str):
        # TODO: modify this to also support local file
        if region_tag.startswith("aws"):
            from skyplane.obj_store.s3_interface import S3Interface

            return S3Interface(bucket)
        elif region_tag.startswith("gcp"):
            from skyplane.obj_store.gcs_interface import GCSInterface

            return GCSInterface(bucket)
        elif region_tag.startswith("azure"):
            from skyplane.obj_store.azure_blob_interface import AzureBlobInterface

            storage_account, container = bucket.split("/", 1)  # <storage_account>/<container>
            return AzureBlobInterface(storage_account, container)

        elif region_tag.startswith("ibmcloud"):
            from skyplane.obj_store.cos_interface import COSInterface

            return COSInterface(bucket, region_tag)
        elif region_tag.startswith("hdfs"):
            from skyplane.obj_store.hdfs_interface import HDFSInterface

            logger.fs.debug(f"attempting to create hdfs bucket {bucket}")
            return HDFSInterface(host=bucket)
        elif region_tag.startswith("local"):
            # from skyplane.obj_store.file_system_interface import FileSystemInterface
            from skyplane.obj_store.posix_file_interface import POSIXInterface

            return POSIXInterface(bucket)
        elif region_tag.startswith("cloudflare"):
            from skyplane.obj_store.r2_interface import R2Interface

            account, bucket = bucket.split("/", 1)  # <storage_account>/<container>
            return R2Interface(account, bucket)
        else:
            raise ValueError(f"Invalid region_tag {region_tag} - could not create interface")
