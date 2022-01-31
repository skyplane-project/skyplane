import argparse
from datetime import datetime
import pickle
import tempfile
from typing import List
import uuid

import boto3
import cvxpy as cp
import numpy as np
import pandas as pd
import ray
from tqdm import tqdm

from skylark import GB, skylark_root
from skylark.replicate.solver import ThroughputProblem, ThroughputSolverILP
from skylark.utils import logger


def get_futures(futures, desc="Jobs", progress_bar=True):
    if progress_bar:
        results = []
        with tqdm(total=len(futures), desc=desc) as pbar:
            while len(futures):
                done_results, futures = ray.wait(futures)
                results.extend(ray.get(done_results))
                pbar.update((len(done_results)))
        return results
    else:
        return ray.get(futures)


@ray.remote
def benchmark(p: ThroughputProblem, throughput_path: str):
    solver = ThroughputSolverILP(throughput_path)
    solution = solver.solve_min_cost(
        p=p,
        solver=cp.GUROBI,
        solver_verbose=False,
    )
    solution.problem.const_throughput_grid_gbits = None
    solution.problem.const_cost_per_gb_grid = None
    return solution


def main(args):
    timestamp = datetime.now().strftime("%Y.%m.%d_%H.%M")
    experiment_tag = f"{timestamp}_{uuid.uuid4()}"
    s3 = boto3.resource("s3").Bucket(args.bucket)
    out_bucket_path = "experiments/pareto_fixed/{}".format(experiment_tag)
    logger.info(f"Writing results to s3://{args.bucket}/{out_bucket_path}")

    throughput_path = skylark_root / "profiles" / "throughput.csv"
    solver = ThroughputSolverILP(throughput_path)
    regions = solver.get_regions()
    regions = np.random.choice(regions, size=6, replace=False)
    
    logger.info("Building problem...")
    problems = []
    for src in regions:
        for dst in regions:
            if src != dst:
                for instance_limit in [1, 2, 4]:
                    min_throughput_range = solver.get_path_throughput(src, dst) / GB * instance_limit
                    max_throughput_range = args.max_throughput * instance_limit
                    for min_throughput in np.linspace(min_throughput_range, max_throughput_range, args.num_throughputs):
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

    batches = [problems[i : i + args.batch_size] for i in range(0, len(problems), args.batch_size)]
    ray.init(address=args.ray_ip)
    for batch_idx, batch in enumerate(batches):
        logger.info(f"Running batch {batch_idx} of {len(batches)}, with {len(batch)} problems")
        refs = [benchmark.remote(prob, throughput_path) for prob in batch]
        results = get_futures(refs)

        n_feasible, n_infeasible = len([r for r in results if r.is_feasible]), len([r for r in results if not r.is_feasible])
        logger.info(f"Got {len(results)} results, {n_feasible} feasible, {n_infeasible} infeasible")

        # save results for batch
        with tempfile.NamedTemporaryFile(mode="wb", delete=True) as f:
            logger.info(f"{batch_idx}/{len(batches)}] Saving solutions for batch {batch_idx} to temp file {f.name}")
            pickle.dump(results, f)
            f.flush()
            s3_out_path = f"pareto_data/{experiment_tag}/{batch_idx}.pkl"
            s3.upload_file(str(f.name), s3_out_path)
            logger.info(f"{batch_idx}/{len(batches)}] Saved batch to s3://{args.bucket}/{s3_out_path}")
    ray.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-throughput", type=float, default=12.5)
    parser.add_argument("--num-throughputs", type=int, default=40)
    parser.add_argument("--gbyte-to-transfer", type=float, default=1)
    parser.add_argument("--bucket", type=str, default="skylark-optimizer-results")
    parser.add_argument("--ray-ip", type=str, default=None)
    parser.add_argument("--batch-size", type=int, default=1024 * 4)
    args = parser.parse_args()
    main(args)
