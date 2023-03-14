from skyplane.obj_store.object_store_interface import ObjectStoreInterface


class ObjectStore:
    def __init__(self) -> None:
        pass

    def download_object(self, bucket_name: str, provider: str, key: str, filename: str):
        obj_store = ObjectStoreInterface.create(f"{provider}:infer", bucket_name)
        obj_store.download_object(key, filename)

    def upload_object(self, filename: str, bucket_name: str, provider: str, key: str):
        obj_store = ObjectStoreInterface.create(f"{provider}:infer", bucket_name)
        obj_store.upload_object(filename, key)

    def exists(self, bucket_name: str, provider: str, key: str) -> bool:
        obj_store = ObjectStoreInterface.create(f"{provider}:infer", bucket_name)
        return obj_store.exists(key)

    def bucket_exists(self, bucket_name: str, provider: str) -> bool:
        # azure not implemented
        if provider == "azure":
            raise NotImplementedError(f"Provider {provider} not implemented")

        obj_store = ObjectStoreInterface.create(f"{provider}:infer", bucket_name)
        return obj_store.bucket_exists()

    def create_bucket(self, region: str, bucket_name: str):
        provider = region.split(":")[0]
        # azure not implemented
        if provider == "azure":
            raise NotImplementedError(f"Provider {provider} not implemented")

        obj_store = ObjectStoreInterface.create(region, bucket_name)
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

        obj_store = ObjectStoreInterface.create(f"{provider}:infer", bucket_name)
        obj_store.delete_bucket()
