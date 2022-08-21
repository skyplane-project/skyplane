# Benchmarks

## Benchmark 1: Large file benchmark

In this benchmark, we measure the impact of *striping* on large file transfers. We transfer a single large file between two AWS regions. Overall, Skyplane is **113.4x faster** than the AWS DataSync for the largest tested file transfer.

<iframe width="835" height="433" seamless frameborder="0" scrolling="no" src="https://docs.google.com/spreadsheets/d/e/2PACX-1vQIAHMHgwgHW7l0s8Zb5z-oYhZloOcfPsQzWKUheY6hPkDtKsSmn3RvPBDWNyqH0Jok1x2MZgmlJ6j1/pubchart?oid=467969270&amp;format=interactive"></iframe>

Benchmark setup:
* Source: AWS ap-southeast-2 (Sydney)
* Destination: AWS eu-west-3 (Paris)
* Number of files: 1x
* File size: {4, 8, 16, 32, 64}GB
* Number of Skyplane VMs: 8

## Benchmark 2: ImageNet transfer on AWS
![DataSync_data_transfer](_static/benchmark/DataSync_data_transfer.png)

## Benchmark 3: ImageNet transfer on GCP
![gcp_data_transfer](_static/benchmark/gcp_data_transfer.png)

## Replicating benchmarks
To replicate select benchmarks, see the following guide:
```{toctree}

benchmark_replicate
```