# Benchmarks

This page compares the performance of Skyplane and cloud data transfer tools such as AWS DataSync and Google Cloud Data Transfer. Achieved results vary depending on the region tested, the size of the file, and the number of VMs used.

## Benchmark: Large file benchmark

In this benchmark, we measure the impact of *striping* on large file transfers. We transfer a single large file between two AWS regions. Overall, Skyplane is **113.4x faster** than the AWS DataSync for the largest tested file transfer.

![Large file benchmark](_static/benchmark/header_speed_plot.png)

<iframe width="835" height="433" seamless frameborder="0" scrolling="no" src="https://docs.google.com/spreadsheets/d/e/2PACX-1vQIAHMHgwgHW7l0s8Zb5z-oYhZloOcfPsQzWKUheY6hPkDtKsSmn3RvPBDWNyqH0Jok1x2MZgmlJ6j1/pubchart?oid=467969270&amp;format=interactive"></iframe>

Benchmark setup:
* Source: AWS ap-southeast-2 (Sydney)
* Destination: AWS eu-west-3 (Paris)
* Number of files: 1x
* File size: {4, 8, 16, 32, 64}GB
* Number of Skyplane VMs: 8

## Benchmark: Cost comparison w/ compression

![Cost comparison w/ compression](_static/benchmark/header_cost_plot.png)

## Benchmark: ImageNet transfer on AWS
![DataSync_data_transfer](_static/benchmark/DataSync_data_transfer.png)

## Benchmark: ImageNet transfer on GCP
![gcp_data_transfer](_static/benchmark/gcp_data_transfer.png)

## Instructions to replicate benchmarks
To replicate select benchmarks, see the following guide:
```{toctree}

benchmark_replicate
```