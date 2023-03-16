**🔥 Blazing fast bulk data transfers between any cloud 🔥**

```
pip install skyplane[aws]
skyplane init
skyplane [sync/cp] [local/s3/gs/azure]://mybucket/big_dataset [local/s3/gs/azure]://mybucket2/
```

Skyplane is a tool for blazingly fast bulk data transfers between object stores in the cloud. It provisions a fleet of VMs in the cloud to transfer data in parallel while using compression and bandwidth tiering to reduce cost.

Skyplane is:
1. 🔥 Blazing fast ([110x faster than AWS DataSync](https://skyplane.org/en/latest/benchmark.html))
2. 🤑 Cheap (4x cheaper than rsync)
3. 🌐 Universal (AWS, Azure and GCP)

You can use Skyplane to transfer data: 
* between object stores within a cloud provider (e.g. AWS us-east-1 to AWS us-west-2)
* between object stores across multiple cloud providers (e.g. AWS us-east-1 to GCP us-central1)
* between local storage and cloud object stores (experimental)

Skyplane currently supports the following source and destination endpoints (any source and destination can be combined): 

| Endpoint           | Source             | Destination        |
|--------------------|--------------------|--------------------|
| AWS S3             | ✅                 | ✅                 |
| Google Storage     | ✅                 | ✅                 |
| Azure Blob Storage | ✅                 | ✅                 |
| Local Disk         | ✅                 | (in progress)      |

Skyplane is an actively developed project. It will have 🔪 SHARP EDGES 🔪. Please file an issue or ask the contributors via [the #help channel on our Slack](https://join.slack.com/t/skyplaneworkspace/shared_invite/zt-1cxmedcuc-GwIXLGyHTyOYELq7KoOl6Q) if you encounter bugs.