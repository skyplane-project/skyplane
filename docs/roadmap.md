# Roadmap

Skyplane is an actively developed project. It will have 🔪 SHARP EDGES 🔪. Please file an issue if you encounter bugs.

If you are interested in contributing, please see our [contributor guide](contributing).

## Skyplane 0.1 release (current)
* `skyplane cp` and `skyplane sync` CLI commands to move bulk (>10GB) of data point-to-point.
* Support for intra-cloud (two regions in same cloud) and inter-cloud copies for AWS, GCP and Azure clouds.
* Large object transfer support for AWS and GCP.
* Compression to reduce egress costs.
* End-to-end encryption and optional socket TLS encryption.
* Firewalled VM-to-VM VPCs for AWS and GCP.

## Skyplane 0.2 release
* Firewalled VM-to-VM VPCs for Azure.
* Large object transfer support for Azure.
* Integration testing suite in CI.

## Upcoming
* Improvements to the scalability of transfers with many files (>10000).
* On-prem transfer support (design needed).
* Broadcast support to multiple destinations.