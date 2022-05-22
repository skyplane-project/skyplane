import argparse
import pickle
import tempfile
import time
import uuid
from datetime import datetime

import boto3
import cvxpy as cp
import numpy as np
import ray
from tqdm import tqdm

from skyplane import GB, skyplane_root
from skyplane.replicate.solver import ThroughputProblem, ThroughputSolution, ThroughputSolverILP
from skyplane.utils import logger
from skyplane.utils.utils import Timer


def get_futures(futures, desc="Jobs", batch_size=64, timeout_s=None):
    results = []
    last_result_time = time.time()
    with tqdm(total=len(futures), desc=desc) as pbar:
        while len(futures):
            done_results, futures = ray.wait(futures, num_returns=min(len(futures), batch_size), timeout=5)
            results.extend(ray.get(done_results))
            pbar.update(len(results) - pbar.n)
            if len(done_results) > 0:
                last_result_time = time.time()
            else:
                if timeout_s is not None and time.time() - last_result_time > timeout_s:
                    logger.info(f"{desc} timed out, stopping")
                    break
                logger.debug(
                    f"{desc} not finished, waiting... {len(futures)} remaining, {len(results)} done, {time.time() - last_result_time}s elapsed since last result"
                )
    logger.info(f"Got {len(results)} results")
    return results


@ray.remote
def benchmark(p: ThroughputProblem, throughput_path: str) -> ThroughputSolution:
    solver = ThroughputSolverILP(throughput_path)
    solution = solver.solve_min_cost(
        p=p,
        solver=cp.CBC,
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

    throughput_path = skyplane_root / "profiles" / "throughput.csv"
    solver = ThroughputSolverILP(throughput_path)
    regions = solver.get_regions()

    logger.info("Building problem...")
    problems = []
    for src in regions:
        for dst in regions:
            if src != dst:
                for instance_limit in [1, 2, 4]:
                    for instance_cost_multiplier in [1, 2]:
                        min_throughput_range = solver.get_path_throughput(src, dst) / GB * instance_limit
                        if src.startswith("aws"):
                            max_throughput_range = 5 * instance_limit
                        elif src.startswith("gcp"):
                            max_throughput_range = 7 * instance_limit
                        elif src.startswith("azure"):
                            max_throughput_range = 12.5 * instance_limit
                        for min_throughput in np.linspace(min_throughput_range - 0.01, max_throughput_range + 0.01, args.num_throughputs):
                            if min_throughput > 0:
                                problems.append(
                                    ThroughputProblem(
                                        src=src,
                                        dst=dst,
                                        required_throughput_gbits=min_throughput,
                                        gbyte_to_transfer=args.gbyte_to_transfer,
                                        instance_limit=instance_limit,
                                        instance_cost_multiplier=instance_cost_multiplier,
                                        instance_provision_time_s=0,
                                    )
                                )
    logger.info(f"Done building problem, {len(problems)} jobs to run")

    batches = [problems[i : i + args.batch_size] for i in range(0, len(problems), args.batch_size)]
    ray.init(address=args.ray_ip)
    count = 0
    with Timer() as t:
        for batch_idx, batch in enumerate(batches):
            np.random.shuffle(batch)
            logger.info(f"[{batch_idx}/{len(batches)}] Running batch {batch_idx} of {len(batches)}, with {len(batch)} problems")
            refs = [benchmark.remote(prob, throughput_path) for prob in batch]
            results = get_futures(refs, desc=f"[{batch_idx}/{len(batches)}] Solve", timeout_s=60)

            n_feasible, n_infeasible = len([r for r in results if r.is_feasible]), len([r for r in results if not r.is_feasible])
            count += len(results)
            logger.info(f"[{batch_idx}/{len(batches)}] Got {len(results)} results, {n_feasible} feasible, {n_infeasible} infeasible")

            seconds_per_problem = t.elapsed / count
            eta = seconds_per_problem * (len(problems) - count)
            eta_h, eta_s = divmod(eta, 3600)
            eta_m, eta_s = divmod(eta_s, 60)
            logger.info(
                f"[{batch_idx}/{len(batches)}] Current ETA: {int(eta_h)}:{int(eta_m)}:{int(eta_s)} ({1. / seconds_per_problem:.2f} problems/s)"
            )

            # save results for batch
            with tempfile.NamedTemporaryFile(mode="wb", delete=True) as f:
                logger.info(f"[{batch_idx}/{len(batches)}] Saving solutions for batch {batch_idx} to temp file {f.name}")
                pickle.dump(results, f)
                f.flush()
                s3_out_path = f"pareto_data/{experiment_tag}/{batch_idx}.pkl"
                s3.upload_file(str(f.name), s3_out_path)
                logger.info(f"[{batch_idx}/{len(batches)}] Saved batch to s3://{args.bucket}/{s3_out_path}")
        logger.info(f"DONE in {t.elapsed:.2f} seconds")
        logger.info(f"Experiment tag: {experiment_tag}")
    ray.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-throughputs", type=int, default=50)
    parser.add_argument("--gbyte-to-transfer", type=float, default=1)
    parser.add_argument("--bucket", type=str, default="skyplane-optimizer-results")
    parser.add_argument("--ray-ip", type=str, default=None)
    parser.add_argument("--batch-size", type=int, default=512)
    args = parser.parse_args()
    main(args)
