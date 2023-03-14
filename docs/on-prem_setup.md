# On-Prem Transfers

Currently Skyplane supports On-prem from local disk, NFS, HDFS to cloud storages. 

## HDFS Setup

Skyplane utilizes Pyarrow and libhdfs for HDFS connection. 

**Transfer from HDFS requires prior Hadoop and Java installation.**

* Please refer to [Pyarrow HDFS documentation](https://arrow.apache.org/docs/python/filesystems.html#hadoop-distributed-file-system-hdfs) for necessary environment variable setup.

Note that the cluster needs to communicate to the Skyplane gateway. Please change the incoming firewall for the clusters to allow traffic from Skyplane. 

### Resolving HDFS Datanodes

A file called `hostname` is under the `./skyplane/scripts/on_prem` folder. This file will be used for hostname/datanode IP resolution. This is for datanode's internal IP resolution.

* Copy the hostname/Internal IP for each datanode and the external ip for the corresponding datanode to the file.

* The hostname after writing all the required information should look like this. 
```text 
<External IP>   <Datanodes' Hostname or Internal IP>
```



### Testing the Transfer

Now you can test running `skyplane cp` to transfer from local disk or HDFS cluster to any cloud storages.


```bash

   ---> Copy from local disk
   $ skyplane cp -r /path/to/local/file gs://...

   ---> Copy from HDFS
   $ skyplane cp -r hdfs://... gs://...

```