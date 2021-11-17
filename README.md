# Skylark: A Unified Data Layer for the Multi-Cloud

<img src="https://gist.githubusercontent.com/parasj/d67e6e161ea1329d4509c69bc3325dcb/raw/232009efdeb8620d2acb91aec111dedf98fdae18/skylark.jpg" width="350px">
Skylark is lifting cloud object stores to the Sky.

## Abstract
The current cloud object store (think S3) is broken. Its core design has not changed in 15 years. It does not support global applications nor does it satisfy the performance requirements of data-intensive applications such as large-scale ML training. The Skylark project aims to address these issues by providing a unified namespace for a multi-cloud object store.

I'll discuss the design of the Skylark Replicator, a system that teleports data between clouds at lightning speed. Specifically, it leverages (1) parallelism during transfers to optimize copies, (2) demand paging to overlap computation with communication and (3) multi-region path optimization to route transfers around points of congestion over the WAN.