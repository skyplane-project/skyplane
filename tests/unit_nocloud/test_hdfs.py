from skyplane.obj_store.hdfs_interface import HDFSFile, HDFSInterface
import filecmp
from skyplane.utils.definitions import MB
import argparse
import os

def test_HDFS(hdfs_host):
    hdfs = HDFSInterface(hdfs_host)
    #create local file and check equivalence
    os.system(f"dd if=/dev/zero of=test1.txt bs={128*MB} count=1")
    assert hdfs.upload_object("test1.txt", "test.txt")
    
    assert hdfs.exists("test.txt")
    
    assert hdfs.download_object("test.txt", "test2.txt")
    
    assert filecmp.cmp("test1.txt", "test2.txt")
    
    assert hdfs.delete_objects(["test.txt"])
    
    os.remove("test1.txt")
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("hdfs", help="source region")
    args = parser.parse_args()
    
    test_HDFS(args.hdfs)
    
    