import argparse
from datetime import datetime
import uuid
import boto3
import pickle
import pickle
from typing import List

import cvxpy as cp
import numpy as np
import pandas as pd
import ray
from tqdm import tqdm

from skylark import GB, skylark_root
from skylark.replicate.solver import ThroughputProblem, ThroughputSolution, ThroughputSolverILP


@ray.remote
def benchmark(p: ThroughputProblem, throughput_path: str) -> ThroughputSolution:
    solver = ThroughputSolverILP(throughput_path)
    solution = solver.solve_min_cost(p=p, solver=cp.CBC, solver_verbose=False)
    solution.problem.const_throughput_grid_gbits = None
    solution.problem.const_cost_per_gb_grid = None
    return solution


def main(args):
    ray.init()
    # ray.init(address="auto")

    # save dir
    timestamp = datetime.now().strftime("%Y.%m.%d_%H.%M")
    experiment_tag = f"{timestamp}_{uuid.uuid4()}"
    s3 = boto3.resource("s3").Bucket(args.bucket)
    out_dir = skylark_root / "data" / "benchmark" / "pareto_speedups" / experiment_tag
    out_dir.mkdir(parents=True, exist_ok=True)
    file_idx = 0

    solver = ThroughputSolverILP(args.throughput_path)
    regions = solver.get_regions()
    regions = np.random.choice(regions, size=3, replace=False)

    problems = []
    for src in regions:
        for dst in regions:
            if src != dst:
                for instance_limit in [1, 2, 4]:
                    for min_throughput in np.linspace(0, args.max_throughput * instance_limit, args.num_throughputs):
                        if min_throughput > 0:
                            problems.append(
                                ThroughputProblem(
                                    src=src,
                                    dst=dst,
                                    required_throughput_gbits=min_throughput,
                                    gbyte_to_transfer=args.gbyte_to_transfer,
                                    instance_limit=instance_limit,
                                )
                            )

    results = []
    for problem in tqdm(problems, desc="dispatch"):
        results.append(benchmark.remote(problem, args.throughput_path))

    # get batches of results with ray.get, update tqdm progress bar
    n_feasible, n_infeasible = 0, 0
    remaining_refs = results
    results_out = []
    with tqdm(total=len(results), desc="Solve") as pbar:
        while remaining_refs:
            ready_refs, remaining_refs = ray.wait(remaining_refs, num_returns=1)
            sol: List[ThroughputSolution] = ray.get(ready_refs)
            results_out.extend(sol)
            pbar.update(len(ready_refs))
            n_feasible += len([s for s in sol if s.is_feasible])
            n_infeasible += len([s for s in sol if not s.is_feasible])
            pbar.set_postfix(feasible=n_feasible, infeasible=n_infeasible)

            # save results to output periodically
            if len(results_out) > 500:
                out_fname = out_dir / f"{file_idx}.pkl"
                relative_out_fname = str(out_fname.relative_to(skylark_root))
                with open(out_fname, "wb") as f:
                    pickle.dump(results_out, f)
                s3.upload_file(str(out_fname), relative_out_fname)
                print(f"Saved {len(results_out)} results to s3://{args.bucket}/{out_fname}")
                results_out = []
                file_idx += 1

    # save remaining results
    if len(results_out) > 0:
        out_fname = out_dir / f"{file_idx}.pkl"
        relative_out_fname = str(out_fname.relative_to(skylark_root))
        with open(out_fname, "wb") as f:
            pickle.dump(results_out, f)
        s3.upload_file(str(out_fname), relative_out_fname)
        print(f"Saved {len(results_out)} results to s3://{args.bucket}/{out_fname}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--throughput-path", type=str, default=skylark_root / "profiles" / "throughput.csv")
    parser.add_argument("--max-throughput", type=float, default=12.5)
    parser.add_argument("--num-throughputs", type=int, default=50)
    parser.add_argument("--gbyte-to-transfer", type=float, default=1)
    parser.add_argument("--bucket", type=str, default="skylark-optimizer-results")
    args = parser.parse_args()
    main(args)
