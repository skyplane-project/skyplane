import argparse

import ray

import pandas as pd
import numpy as np
import cvxpy as cp
import matplotlib.pyplot as plt
from loguru import logger
from tqdm import tqdm

from skylark import skylark_root
from skylark.solver import ThroughputSolverILP


@ray.remote
def benchmark(path, src, dst, min_throughput, gbyte_to_transfer=1):
    solver = ThroughputSolverILP(path)
    solution = solver.solve(src, dst, required_throughput_gbits=min_throughput, solver=cp.GUROBI)
    if solution["feasible"]:
        return dict(src=src, dst=dst, min_throughput=min_throughput, cost=solution["cost"], throughput=solution["throughput"])
    else:
        return None


def main(args):
    ray.init()
    solver = ThroughputSolverILP(args.cost_path)
    regions = solver.get_regions()

    configs = []
    for src in regions:
        for dst in regions:
            for min_throughput in np.linspace(0, args.max_throughput, args.num_throughputs)[1:]:
                configs.append((src, dst, min_throughput))

    results = []
    for src, dst, min_throughput in tqdm(configs, desc="dispatch"):
        if src == dst:
            continue
        result = benchmark.remote(args.cost_path, src, dst, min_throughput)
        results.append(result)

    # get batches of results with ray.get, update tqdm progress bar
    ready_refs, remaining_refs = [], results
    with tqdm(total=len(results)) as pbar:
        while remaining_refs:
            ready_refs, remaining_refs = ray.wait(remaining_refs, num_returns=1)
            pbar.update(len(ready_refs))

    results = ray.get(ready_refs)
    results = [r for r in results if r is not None]
    df = pd.DataFrame(results)
    df.to_csv(skylark_root / "data" / "out.csv", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cost-path", type=str, default=str(skylark_root / "data" / "throughput" / "df_throughput_agg.csv"))
    parser.add_argument("--max-throughput", type=float, default=10)
    parser.add_argument("--num-throughputs", type=int, default=20)
    parser.add_argument("--gbyte-to-transfer", type=float, default=1)
    args = parser.parse_args()
    main(args)
