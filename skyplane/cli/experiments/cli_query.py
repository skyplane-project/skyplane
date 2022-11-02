from importlib.resources import path

import typer

from skyplane.replicate.solver import ThroughputSolver


def util_grid_throughput(
    src: str,
    dest: str,
    src_tier: str = "PREMIUM",
    dest_tier: str = "PREMIUM",
):
    with path("skyplane.data", "throughput.csv") as throughput_grid_path:
        solver = ThroughputSolver(throughput_grid_path)
        print(solver.get_path_throughput(src, dest, src_tier, dest_tier) / 2**30)


def util_grid_cost(
    src: str,
    dest: str,
    src_tier: str = "PREMIUM",
    dest_tier: str = "PREMIUM",
):
    with path("skyplane.data", "throughput.csv") as throughput_grid_path:
        solver = ThroughputSolver(throughput_grid_path)
        print(solver.get_path_cost(src, dest, src_tier, dest_tier))


def get_max_throughput(region_tag: str):
    provider = region_tag.split(":")[0]
    if provider == "aws":
        print(5)
    elif provider == "gcp":
        print(7)
    elif provider == "azure":
        print(16)
    else:
        typer.secho(f"Unknown provider: {provider}", fg="red", err=True)
        raise typer.Exit(1)


def dump_full_util_cost_grid():
    with path("skyplane.data", "throughput.csv") as throughput_grid_path:
        solver = ThroughputSolver(throughput_grid_path)
        regions = solver.get_regions()

        print("src,dest,src_tier,dest_tier,cost")
        for src in regions:
            for dest in regions:
                for src_tier in ["PREMIUM", "STANDARD"]:
                    for dest_tier in ["PREMIUM", "STANDARD"]:
                        try:
                            print(f"{src},{dest},{src_tier},{dest_tier},{solver.get_path_cost(src, dest, src_tier, dest_tier)}")
                        except AssertionError:
                            pass
