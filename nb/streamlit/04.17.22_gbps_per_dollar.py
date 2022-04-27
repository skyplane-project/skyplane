import subprocess
import os
from pathlib import Path
import pickle
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt
from skylark.replicate.solver import ThroughputSolver
import streamlit as st
from nb.streamlit.common import load_data
from skylark import skylark_root

import numpy as np

st.sidebar.header("Dashboard: Gbps per dollar")

data_dir = skylark_root / "data"
figure_dir = data_dir / "figures" / "pareto_speedups_updated"
figure_dir.mkdir(exist_ok=True, parents=True)

avail_style = list(plt.style.available)
avail_cmap = list(plt.colormaps())
col1, col2 = st.sidebar.columns(2)
style = col1.selectbox("Style", avail_style, avail_style.index("seaborn-bright"))
cmap = col2.selectbox("Color map", avail_cmap, avail_cmap.index("plasma"))
plt.style.use(style)
plt.set_cmap(cmap)
plt.rcParams["axes.labelweight"] = "bold"

with st.spinner("Loading data"):
    df, out_dir = load_data()
    df["throughput_speedup"] = df["throughput_achieved_gbits"] / df["baseline_throughput_achieved_gbits"]
    df["cost_increase"] = df["cost_total"] / df["baseline_cost_total"]
    df["src_provider"] = df["problem_src"].apply(lambda x: x.split(":")[0])
    df["dst_provider"] = df["problem_dst"].apply(lambda x: x.split(":")[0])
    df["gbps_per_dollar"] = df["throughput_achieved_gbits"] / df["cost_total"]

src_regions = sorted(df.problem_src.unique())
dst_regions = sorted(df.problem_dst.unique())
n_instance_options = list(sorted(df["problem_instance_limit"].unique().astype(np.int32)))
n_instances = int(st.sidebar.selectbox("Number of instances", n_instance_options, n_instance_options.index(1)))
df_selected = df[df["problem_instance_limit"] == n_instances]


@st.cache
def load_latency_df():
    latency_df = pd.read_csv(skylark_root / "profiles" / "latency.csv")
    return latency_df


latency_df = load_latency_df().copy()


@st.cache(suppress_st_warning=True, allow_output_mutation=True)
def compute_one_hop():
    solver = ThroughputSolver(skylark_root / "profiles" / "throughput.csv")
    src_regions = solver.get_regions()
    dest_regions = solver.get_regions()
    inter_regions = solver.get_regions()

    costs = []
    for src_region_select in tqdm(src_regions):
        with tqdm(dest_regions) as dest_progress:
            for dest_region_input in dest_progress:
                dest_progress.set_description(f"{src_region_select} -> {dest_region_input}")
                costs_pair = []
                for inter_region in inter_regions:
                    if inter_region == src_region_select or inter_region == dest_region_input:
                        continue
                    cost = solver.get_path_cost(src_region_select, inter_region) + solver.get_path_cost(inter_region, dest_region_input)
                    throughput = min(
                        solver.get_path_throughput(src_region_select, inter_region),
                        solver.get_path_throughput(inter_region, dest_region_input),
                    )
                    costs_pair.append(
                        {
                            "inter": inter_region,
                            "cost": cost,
                            "throughput": throughput / 2**30,
                            "src": src_region_select,
                            "dst": dest_region_input,
                        }
                    )
                # select max throughput dict from costs_pair and append to costs
                costs.append(max(costs_pair, key=lambda x: x["throughput"]))
    df = pd.DataFrame(costs).copy()
    return df


df_one_hop = compute_one_hop().copy()
df_one_hop["gbps_per_dollar"] = df_one_hop["throughput"] * max(df_selected["problem_instance_limit"]) / df_one_hop["cost"]

# # merge latency data to df
# # src_region, dst_reigon -> problem_src_region, problem_dst_region
# latency_df['problem_src_region'] = latency_df['src_region']
# latency_df['problem_dst_region'] = latency_df['dst_region']
# df_selected = df_selected.merge(latency_df, how="left", on=["problem_src_region", "problem_dst_region"])

# # merge one hop data to df
# # src, dst -> problem_src_region, problem_dst_region
# latency_df['src'] = latency_df['src_region']
# latency_df['dst'] = latency_df['dst_region']
# df_one_hop = df_one_hop.merge(latency_df, how="left", on=["src", "dst"])

df_one_hop["gbps_per_dollar"] = df_one_hop["gbps_per_dollar"] / 70 / 8  # imagenet
df_selected["gbps_per_dollar"] = df_selected["gbps_per_dollar"] / 70 / 8  # imagenet

fig, ax = plt.subplots(figsize=(8, 6))
# throughput_achieved_gbits versus cost_total
df_one_hop.plot(x="throughput", y="gbps_per_dollar", ax=ax, kind="scatter", s=10, alpha=0.5, label="RON", color="red")
df_selected.plot(
    x="throughput_achieved_gbits", y="gbps_per_dollar", ax=ax, kind="scatter", s=10, alpha=0.5, label="Overcloud", color="blue"
)
ax.set_ylabel("Gbps per dollar")

fig.tight_layout()
st.pyplot(fig)

# subsample df_selected to ['src', 'dst', 'min_rtt', 'gbps_per_dollar']
df_selected_sub = df_selected[
    ["problem_src_region", "problem_dst_region", "gbps_per_dollar", "cost_total", "throughput_achieved_gbits"]
].copy()
df_selected_sub.rename(
    columns={
        "problem_src_region": "src",
        "problem_dst_region": "dst",
        "gbps_per_dollar": "skylark_gbps_per_dollar",
        "cost_total": "skylark_cost_total",
        "throughput_achieved_gbits": "skylark_throughput",
    },
    inplace=True,
)
df_selected_sub = df_selected_sub.dropna().set_index(["src", "dst"])

# subsample df_one_hop to ['src', 'dst', 'min_rtt', 'gbps_per_dollar']
# st.write("One hop column names:" + str(df_one_hop.columns))
df_one_hop_sub = df_one_hop[["src", "dst", "gbps_per_dollar", "cost", "throughput"]].copy()
df_one_hop_sub.rename(columns={"gbps_per_dollar": "ron_gbps_per_dollar", "cost": "ron_cost", "throughput": "ron_throughput"}, inplace=True)
df_one_hop_sub = df_one_hop_sub.dropna().set_index(["src", "dst"])

# merge df_selected_sub and df_one_hop_sub by index
df = df_selected_sub.merge(df_one_hop_sub, how="left", left_index=True, right_index=True).reset_index()
df["cost_gbps_improvement"] = df["skylark_gbps_per_dollar"] / df["ron_gbps_per_dollar"]
df["cost_improvement"] = df["ron_cost"] / df["skylark_cost_total"]
df["src_cloud"] = df["src"].map(lambda x: x.split(":")[0])
df["dst_cloud"] = df["dst"].map(lambda x: x.split(":")[0])
df = df[df["src_cloud"] != df["dst_cloud"]]
st.dataframe(df)


# plot cost_improvement as box plot
fig, ax = plt.subplots(figsize=(8, 6))
df.plot(x="ron_throughput", y="ron_cost", ax=ax, kind="scatter", s=10, alpha=0.5, color="red", label="RON")
df.plot(x="skylark_throughput", y="skylark_cost_total", ax=ax, kind="scatter", s=10, alpha=0.5, color="blue", label="Overcloud")
ax.set_ylabel("Cost ($/GB)")
ax.set_xlabel("Throughput (Gbps)")
fig.tight_layout()
st.pyplot(fig)

st.dataframe(df_one_hop_sub)
