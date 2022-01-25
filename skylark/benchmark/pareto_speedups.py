import argparse

import cvxpy as cp
import numpy as np
import pandas as pd
import ray
from tqdm import tqdm

from skylark import GB, skylark_root
from skylark.replicate.solver import ThroughputSolverILP


@ray.remote
def benchmark(
    src,
    dst,
    min_throughput,
    gbyte_to_transfer=1,
    instance_limit=1,
    max_connections_per_path=64,
    max_connections_per_node=64,
    log_dir=None,
):
    solver = ThroughputSolverILP(skylark_root / "profiles" / "throughput_mini.csv")
    solution = solver.solve_min_cost(
        src,
        dst,
        required_throughput_gbits=min_throughput * instance_limit,
        gbyte_to_transfer=gbyte_to_transfer,
        instance_limit=instance_limit,
        benchmark_throughput_connections=max_connections_per_path,
        max_connections_per_node=max_connections_per_node,
        solver=cp.GUROBI,
        solver_verbose=False,
    )
    if solution["feasible"]:
        baseline_throughput = solver.get_path_throughput(src, dst) / GB
        baseline_cost = solver.get_path_cost(src, dst) * gbyte_to_transfer
        return dict(
            src=src,
            dst=dst,
            min_throughput=min_throughput,
            gbyte_to_transfer=gbyte_to_transfer,
            instance_limit=instance_limit,
            max_connections_per_path=max_connections_per_path,
            max_connections_per_node=max_connections_per_node,
            cost=solution["cost"],
            baseline_cost=baseline_cost,
            throughput=solution["throughput"],
            baseline_throughput=baseline_throughput,
            throughput_speedup=solution["throughput"] / baseline_throughput,
            cost_factor=solution["cost"] / baseline_cost,
            solution=solution,
        )
    else:
        return None


def main(args):
    ray.init()
    solver = ThroughputSolverILP(skylark_root / "profiles" / "throughput_mini.csv")
    regions = solver.get_regions()
    # regions = np.random.choice(regions, size=6, replace=False)

    configs = []
    for src in regions:
        for dst in regions:
            if src != dst:
                for instance_limit in [1, 2, 4]:
                    for min_throughput in np.linspace(0, args.max_throughput * instance_limit, args.num_throughputs)[1:]:
                        configs.append(
                            dict(
                                src=src,
                                dst=dst,
                                min_throughput=min_throughput,
                                gbyte_to_transfer=args.gbyte_to_transfer,
                                instance_limit=instance_limit,
                                max_connections_per_path=64,
                                max_connections_per_node=64,
                            )
                        )

    results = []
    for config in tqdm(configs, desc="dispatch"):
        results.append(benchmark.remote(**config))

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
    df.to_pickle(skylark_root / "data" / "pareto.pkl", index=False)
    print(f"Saved {len(results_out)} results to {skylark_root / 'data' / 'pareto.pkl'}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-throughput", type=float, default=15)
    parser.add_argument("--num-throughputs", type=int, default=40)
    parser.add_argument("--gbyte-to-transfer", type=float, default=1)
    args = parser.parse_args()
    main(args)
