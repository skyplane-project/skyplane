import subprocess
import os
from pathlib import Path
import pickle
import pandas as pd
import matplotlib.pyplot as plt
from skylark.replicate.solver import ThroughputSolver
import streamlit as st
from nb.streamlit.common import load_data
from skylark import skylark_root

solver = ThroughputSolver(skylark_root / "profiles" / "throughput.csv")
src_regions = solver.get_regions()
dest_regions = solver.get_regions()
inter_regions = solver.get_regions()

from tqdm import tqdm

with tqdm(total=len(src_regions) * len(dest_regions)) as pbar:
    for src_region_select in src_regions:
        for dest_region_input in dest_regions:
            pbar.update(1)
            costs = []
            for inter_region in inter_regions:
                if inter_region == src_region_select or inter_region == dest_region_input:
                    continue
                cost = solver.get_path_cost(src_region_select, inter_region) + solver.get_path_cost(inter_region, dest_region_input)
                throughput = min(solver.get_path_throughput(src_region_select, inter_region), solver.get_path_throughput(inter_region, dest_region_input))
                costs.append({'inter': inter_region, 'cost': cost, 'throughput': throughput / 2**30})
            df = pd.DataFrame(costs)

            # plot scatter plot with labels of cost vs throughput
            fig, ax = plt.subplots(figsize=(8, 6))
            ax.scatter(df["throughput"], df["cost"], alpha=0.8)
            ax.set_xlabel("Throughput (GB/s)")
            ax.set_ylabel("Cost (USD)")
            fig.set_facecolor("white")
            
            with skylark_root / "data" / "figures" / "one_hop" / f"{src_region_select}_{dest_region_input}.png" as path:
                fig.savefig(path, bbox_inches="tight")
            
            # close the figure to avoid memory leak
            plt.close(fig)