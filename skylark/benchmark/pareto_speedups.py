import argparse
from datetime import datetime
import tempfile
import uuid
import boto3
import pickle
from typing import List

import cvxpy as cp
import numpy as np
import ray
from tqdm import tqdm

from skylark import GB, skylark_root
from skylark.replicate.solver import ThroughputProblem, ThroughputSolution, ThroughputSolverILP


@ray.remote(num_cpus=1)
def benchmark(p: ThroughputProblem, throughput_path: str, throughput_grid=None, cost_grid=None) -> ThroughputSolution:
    solver = ThroughputSolverILP(throughput_path)
    solution = solver.solve_min_cost(p=p, solver=cp.CBC, solver_verbose=False, throughput_grid=throughput_grid, cost_grid=cost_grid)
    solution.problem.const_throughput_grid_gbits = None
    solution.problem.const_cost_per_gb_grid = None
    return solution


def main(args):
    ray.init(address=args.ray_ip)

    # save dir
    timestamp = datetime.now().strftime("%Y.%m.%d_%H.%M")
    experiment_tag = f"{timestamp}_{uuid.uuid4()}"
    s3 = boto3.resource("s3").Bucket(args.bucket)
    out_dir = skylark_root / "data" / "benchmark" / "pareto_speedups" / experiment_tag
    out_dir.mkdir(parents=True, exist_ok=True)
    file_idx = 0

    solver = ThroughputSolverILP(args.throughput_path)
    regions = solver.get_regions()
    throughput_grid_ref = ray.put(solver.get_throughput_grid())
    cost_grid_ref = ray.put(solver.get_cost_grid())

    problems = []
    for src in regions:
        for dst in regions:
            if src != dst:
                for instance_limit in [1, 2, 4, 8]:
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
    # shuffle problems
    np.random.shuffle(problems)

    with tqdm(total=len(problems), desc="Solve") as pbar:
        n_feasible, n_infeasible, n_pending = 0, 0, len(problems)
        for batch_idx, batch in enumerate([problems[i : i + args.batch_size] for i in range(0, len(problems), args.batch_size)]):
            # dispatch and wait
            refs = [benchmark.remote(p, args.throughput_path, throughput_grid_ref, cost_grid_ref) for p in batch]
            ready_refs, remaining_refs = ray.wait(refs, num_returns=1)
            while remaining_refs:
                ready_refs, remaining_refs = ray.wait(remaining_refs, num_returns=min(len(remaining_refs), 8))
                pbar.update(len(ready_refs))

            # retrieve solutions
            solutions = ray.get(refs)
            n_pending -= len(solutions)
            n_feasible += len([s for s in solutions if s.is_feasible])
            n_infeasible += len([s for s in solutions if not s.is_feasible])
            pbar.set_postfix(feasible=f"{n_feasible}/{n_feasible + n_infeasible}", remaining=n_pending)

            # save results for batch
            with tempfile.NamedTemporaryFile(mode="wb", delete=True) as f:
                pickle.dump(solutions, f)
                f.flush()
                s3_out_path = f"pareto_data/{experiment_tag}/{batch_idx}.pkl"
                s3.upload_file(str(f.name), s3_out_path)
                tqdm.write(f"Saved batch to s3://{args.bucket}/{s3_out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--throughput-path", type=str, default=skylark_root / "profiles" / "throughput.csv")
    parser.add_argument("--max-throughput", type=float, default=12.5)
    parser.add_argument("--num-throughputs", type=int, default=100)
    parser.add_argument("--gbyte-to-transfer", type=float, default=1)
    parser.add_argument("--bucket", type=str, default="skylark-optimizer-results")
    parser.add_argument("--batch-size", type=int, default=1024 * 16)
    parser.add_argument("--ray-ip", type=str, default=None)
    args = parser.parse_args()
    main(args)
