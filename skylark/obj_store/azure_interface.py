import mimetypes
import os
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Iterator, List

import os, uuid, time
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__, BlobBlock

from skylark.obj_store.object_store_interface import NoSuchObjectException, ObjectStoreInterface, ObjectStoreObject


class AzureObject(ObjectStoreObject):
    def full_path(self):
        raise NotImplementedError()


class AzureInterface(ObjectStoreInterface):
    def __init__(self, azure_region, container_name):
        # TODO: infer region?
        self.azure_region = azure_region

        self.container_name = container_name
        self.pending_downloads, self.completed_downloads = 0, 0
        self.pending_uploads, self.completed_uploads = 0, 0

        # Retrieve the connection string for use with the application. The storage
	    # connection string is stored in an environment variable on the machine
	    # running the application called AZURE_STORAGE_CONNECTION_STRING. If the environment variable is
	    # created after the application is launched in a console or with Visual Studio,
	    # the shell or application needs to be closed and reloaded to take the
	    # environment variable into account.
	    self._connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
	    # Create the BlobServiceClient object which will be used to create a container client
	    self.blob_service_client = BlobServiceClient.from_connection_string(_connect_str)

	    self.container_client = None

        # TODO:: Figure this out, since azure by default has 15 workers 
        self.pool = ThreadPoolExecutor(max_workers=1) 

    def _on_done_download(self, **kwargs):
        self.completed_downloads += 1
        self.pending_downloads -= 1

    def _on_done_upload(self, **kwargs):
        self.completed_uploads += 1
        self.pending_uploads -= 1

    def infer_azure_region(self, bucket_name: str):
        raise NotImplementedError()

    def container_exists(self): # More like is container empty
       # Get a client to interact with a specific container - though it may not yet exist
   		if (self.container_client is None):
   			self.container_client = self.blob_service_client.get_container_client(self.container_name)
   		try:
   			for blob in self.container_client.list_blobs():
   				print("Found blob: ", blob.name)
	   		except ResourceNotFoundError:
	       		return False

    def create_container(self, storage_class: str = "STANDARD"):
    	try:
    		self.container_client = self.blob_service_client.create_container(self.container_name)
       		self.properties = self.container_client.get_container_properties()
   		except ResourceExistsError:
       		typer.secho("Container already exists.")
       		exit(-1)

    def delete_container(self):
    	if (self.container_client is None):
   			self.container_client = self.blob_service_client.get_container_client(self.container_name)
    	try:
    		self.container_client.delete_container()
    	except ResourceNotFoundError:
    		typer.secho("Container doesn't exists. Unable to delete")

    def list_objects(self, prefix="") -> Iterator[AzureObject]:
        if (self.container_client is None):
   			self.container_client = self.blob_service_client.get_container_client(self.container_name)
        blobs = self.container_client.list_blobs()
        for blob in blobs:
            yield AzureObject("azure", blob.container, blob.name, blob.size, blob.last_modified)

    def delete_objects(self, keys: List[str]):
    	raise NotImplementedError
        for key in keys:
            self._gcs_client.bucket(self.bucket_name).blob(key).delete()
            assert not self.exists(key)

    def get_obj_metadata(self, obj_name): # Not Tested
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=obj_name)
	   try:
	       return blob_client.get_blob_properties()
	   except ResourceNotFoundError:
	       print("No blob found.")

    def get_obj_size(self, obj_name):
        return self.get_obj_metadata(obj_name).size

    def exists(self, obj_name):
    	blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=obj_name)
        try:
            blob_client.exists()
        except:
            typer.secho("Undefined Behavior.")
            exit(-1)

    # todo: implement range request for download
    def download_object(self, src_object_name, dst_file_path) -> Future:
    	raise NotImplementedError
        src_object_name, dst_file_path = str(src_object_name), str(dst_file_path)
        src_object_name = src_object_name if src_object_name[0] != "/" else src_object_name

        def _download_object_helper(offset, **kwargs):

            bucket = self._gcs_client.bucket(self.bucket_name)
            blob = bucket.blob(src_object_name)
            chunk = blob.download_as_string()

            # write file
            if not os.path.exists(dst_file_path):
                open(dst_file_path, "a").close()
            with open(dst_file_path, "rb+") as f:
                f.seek(offset)
                f.write(chunk)

        return self.pool.submit(_download_object_helper, 0)

    def upload_object(self, src_file_path, dst_object_name, content_type="infer") -> Future:
    	raise NotImplementedError
        src_file_path, dst_object_name = str(src_file_path), str(dst_object_name)
        dst_object_name = dst_object_name if dst_object_name[0] != "/" else dst_object_name
        os.path.getsize(src_file_path)

        if content_type == "infer":
            content_type = mimetypes.guess_type(src_file_path)[0] or "application/octet-stream"

        def _upload_object_helper():
            bucket = self._gcs_client.bucket(self.bucket_name)
            blob = bucket.blob(dst_object_name)
            blob.upload_from_filename(src_file_path)
            return True

        return self.pool.submit(_upload_object_helper)
