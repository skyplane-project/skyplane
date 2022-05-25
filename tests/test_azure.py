import os
import time
import uuid

from azure.storage.blob import BlobServiceClient, __version__

try:
    print("Azure Blob Storage v" + __version__)
    # Retrieve the connection string for use with the application. The storage
    # connection string is stored in an environment variable on the machine
    # running the application called AZURE_STORAGE_CONNECTION_STRING. If the environment variable is
    # created after the application is launched in a console or with Visual Studio,
    # the shell or application needs to be closed and reloaded to take the
    # environment variable into account.
    connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Create a unique name for the container
    container_name = str(uuid.uuid4())
    print(f"Creating container:{container_name}")

    # Create the container
    container_client = blob_service_client.create_container(container_name)
    # Create a local directory to hold blob data
    local_path = "./data"

    # Create a file in the local data directory to upload and download
    local_file_name = "demo.txt"

    upload_file_path = os.path.join(local_path, local_file_name)
    print("\nFile Size (MB):", os.path.getsize(upload_file_path) / (1024 * 1024))

    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

    print("\nUploading to Azure Storage as blob:\n\t" + local_file_name)

    # Upload the created file
    upload_start_time = time.time()
    with open(upload_file_path, "rb") as data:
        blob_client.upload_blob(data)
    print("\nTime to upload from filesys(s):", time.time() - upload_start_time)

    print("\nListing blobs...")

    # List the blobs in the container
    blob_list = container_client.list_blobs()
    for blob in blob_list:
        print("\t" + blob.name)

    # Download the blob to a local file using read_all()
    # Add 'DOWNLOAD' before the .txt extension so you can see both files in the data directory
    download_file_path = os.path.join(local_path, str.replace(local_file_name, ".txt", "DOWNLOAD_READ_ALL.txt"))
    print("\nDownloading blob to \n\t" + download_file_path)

    download_start_time = time.time()
    with open(download_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob(max_concurrency=24).readall())
    print("\nTime to Download and write to file (s):", time.time() - download_start_time)

    # Download the blob to a local file using chunks()
    stream = blob_client.download_blob()
    block_list = []

    download_chunk_start_time = time.time()
    # Reading data in chunks to avoid loading all into memory at once
    for chunk in stream.chunks():
        # process your data (anything can be done here `chunk` is a 4M byte array).
        # print(chunk.decode())
        # block_id = str(uuid.uuid4())
        # blob_client.stage_block(block_id=block_id, data=chunk)
        block_list.append([chunk])

    print("\nTime to download as chunks (s):", time.time() - download_chunk_start_time)

    # Clean up
    print("\nPress the Enter key to begin clean up")
    input()

    print("Deleting blob container...")
    container_client.delete_container()

    print("Deleting the local source and downloaded files...")
    os.remove(download_file_path)

    print("Done")

except Exception as ex:
    print("Exception:")
    print(ex)
