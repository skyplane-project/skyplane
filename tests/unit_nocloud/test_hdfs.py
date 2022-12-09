import time
from skyplane.api.impl.path import parse_path
from skyplane.obj_store.file_system_interface import FileSystemInterface
from skyplane.utils.definitions import MB
import argparse
import os

FILE_SIZE = 125 * MB

def test_setup(provider):
    # Uploading file to read from
    os.system(f"dd if=/dev/zero of=test.txt bs={FILE_SIZE} count=1")
     
    print("Uploading file to read from...")
    assert provider.upload_object("test.txt", "test1.txt")
    assert provider.exists("test1.txt")

def test_cleanup(provider):
    assert provider.delete_objects(["test1.txt"])
    assert not provider.exists("test1.txt")
    os.remove("test1.txt")

def test_read(provider):
    # Reading file. Reading test1.txt from the source to test.txt
    print("Reading file...")
    before= time.time()
    assert provider.download_object("test1.txt", "test.txt")
    after = time.time()
    transfer_time = after-before
    print(f"Test read only finished. Time taken: {transfer_time}, Throughput: {transfer_time/FILE_SIZE}MBps")
    
    
def test_write(provider, destination):
    
    # Uploading file to read from. Writing test.txt to the destination test2.txt
    print("Writing file...")
    before= time.time()
    assert destination.upload_object("test.txt", "test2.txt")
    after = time.time()
    transfer_time = after-before
    
    print(f"Test read write finished. Time taken: {transfer_time}, Throughput: {transfer_time/FILE_SIZE}MBps")
    
    destination.delete_objects(["test2.txt"])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", type=str, required=True)
    parser.add_argument("--dst", type=str, required=False)
    args = parser.parse_args()
    
    read_only = False
    
    src, src_host, src_path = parse_path(args.src)
    provider = FileSystemInterface.create(src, src_host, src_path)
    
    #create local file
    test_setup(provider)
    if args.dst:
        dst, dst_host, dst_path = parse_path(args.dst)
        destination = FileSystemInterface.create(dst, dst_host, dst_path)
        
    #test read and write here
    before = time.time()
    test_read(provider)
    if args.dst:
        test_write(provider, destination)
    after = time.time()
    
    print(f"All tests finished. Time taken: {after-before}, Throughput: {(after-before)/FILE_SIZE}MBps")
    test_cleanup(provider)

    