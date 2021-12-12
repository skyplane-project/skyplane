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
    solution = solver.solve(src, dst, required_throughput_gbits=min_throughput, gbyte_to_transfer=gbyte_to_transfer, solver=cp.GUROBI)
    if solution["feasible"]:
        baseline_throughput = solver.get_path_throughput(src, dst) / 1e9
        baseline_cost = solver.get_path_cost(src, dst) * gbyte_to_transfer
        return dict(src=src, dst=dst, min_throughput=min_throughput,
            cost=solution["cost"], baseline_cost=baseline_cost,
            throughput=solution["throughput"], baseline_throughput=baseline_throughput,
            throughput_speedup=solution["throughput"] / baseline_throughput,
            cost_factor=solution["cost"] / baseline_cost)
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
        result = benchmark.remote(args.cost_path, src, dst, min_throughput, args.gbyte_to_transfer)
        results.append(result)

    # get batches of results with ray.get, update tqdm progress bar
    remaining_refs = results
    results_out = []
    with tqdm(total=len(results)) as pbar:
        while remaining_refs:
            ready_refs, remaining_refs = ray.wait(remaining_refs, num_returns=1)
            results_out.extend(ray.get(ready_refs))
            pbar.update(len(ready_refs))

    results_out = [r for r in results_out if r is not None]
    df = pd.DataFrame(results_out)
    df.to_csv(skylark_root / "data" / "pareto.csv", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cost-path", type=str, default=str(skylark_root / "data" / "throughput" / "df_throughput_agg.csv"))
    parser.add_argument("--max-throughput", type=float, default=10)
    parser.add_argument("--num-throughputs", type=int, default=10)
    parser.add_argument("--gbyte-to-transfer", type=float, default=1)
    args = parser.parse_args()
    main(args)
