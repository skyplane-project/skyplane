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

st.sidebar.header("Dashboard: visualizing RON plans")

solver = ThroughputSolver(skylark_root / "profiles" / "throughput.csv")
src_regions = solver.get_regions()
dest_regions = solver.get_regions()
inter_regions = solver.get_regions()

src_region_select = st.sidebar.selectbox("Source region", src_regions)
dest_region_input = st.sidebar.selectbox("Destination region", dest_regions)

baseline_cost = solver.get_path_cost(src_region_select, dest_region_input)
baseline_throughput = solver.get_path_throughput(src_region_select, dest_region_input)
st.sidebar.write(f"Baseline throughput: {baseline_throughput / 2**30:.2f} Gb/s")
st.sidebar.write(f"Baseline cost: ${baseline_cost:.2f}")

costs = []
for inter_region in inter_regions:
    if inter_region == src_region_select or inter_region == dest_region_input:
        continue
    cost = solver.get_path_cost(src_region_select, inter_region) + solver.get_path_cost(inter_region, dest_region_input)
    throughput = min(
        solver.get_path_throughput(src_region_select, inter_region), solver.get_path_throughput(inter_region, dest_region_input)
    )

    costs.append(
        {
            "inter": inter_region,
            "cost": cost,
            "throughput": throughput / 2**30,
            "cost_overhead": cost / baseline_cost if baseline_cost > 0 else 0,
            "thoughput_speedup": throughput / baseline_throughput if baseline_throughput > 0 else 0,
        }
    )
df = pd.DataFrame(costs)
st.dataframe(df)

# plot scatter plot with labels of cost vs throughput
fig, ax = plt.subplots(figsize=(8, 6))
ax.scatter(df["throughput"], df["cost"], alpha=0.8)
ax.set_xlabel("Throughput (GB/s)")
ax.set_ylabel("Cost (USD)")

ax.axhline(y=solver.get_path_cost(src_region_select, dest_region_input), color="red", linestyle="--")
ax.axvline(x=solver.get_path_throughput(src_region_select, dest_region_input) / 2**30, color="red", linestyle="--")

fig.set_facecolor("white")
st.pyplot(fig, bbox_inches="tight")

st.subheader("Query single hop plan")
inter_region_single = st.selectbox("Intermediate region", inter_regions)

col1, col2, col3 = st.columns(3)
col1.subheader("Direct")
col1.write(f"{src_region_select} -> {dest_region_input}")
col1.write(f"Throughput: {solver.get_path_throughput(src_region_select, dest_region_input) / 2**30:.4f} Gbps")
col1.write(f"Cost: ${solver.get_path_cost(src_region_select, dest_region_input):.02f}")

col2.subheader("Hop 1")
col2.write(f"{src_region_select} -> {inter_region_single}")
col2.write(f"Throughput: {solver.get_path_throughput(src_region_select, inter_region_single) / 2**30:.4f} Gbps")
col2.write(f"Cost: ${solver.get_path_cost(src_region_select, inter_region_single):.02f}")

col3.subheader("Hop 2")
col3.write(f"{inter_region_single} -> {dest_region_input}")
col3.write(f"Throughput: {solver.get_path_throughput(inter_region_single, dest_region_input) / 2**30:.4f} Gbps")
col3.write(f"Cost: ${solver.get_path_cost(inter_region_single, dest_region_input):.02f}")
