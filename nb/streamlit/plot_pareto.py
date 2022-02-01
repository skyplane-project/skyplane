import subprocess
import os
from pathlib import Path
import pickle
import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st
from nb.streamlit.common import load_data
from skylark import skylark_root

data_dir = skylark_root / "data"
figure_dir = data_dir / "figures" / "pareto_speedups_updated"
figure_dir.mkdir(exist_ok=True, parents=True)

plt.style.use("seaborn-bright")
plt.set_cmap("plasma")

df, out_dir = load_data()

# select rows where throughput_achieved_gbits >= baseline_throughput_gbits
# df = df[df["throughput_achieved_gbits"] >= df["baseline_achieved_throughput_gbits"]]

src_regions_choices = sorted(df["problem_src"].unique())
dst_regions_choices = sorted(df["problem_dst"].unique())
instance_choices = sorted(df["problem_instance_limit"].unique())

########
# Plot geomean
########

# for each src, dest pair, compute the geometric mean of throughput speedup
# def calc_geomean(x):
#     if len(x) == 0:
#         return np.nan
#     return np.exp(np.mean(np.log(x)))

# st.write("Geomean speedup")
# fig, ax = plt.subplots(figsize=(8, 6))
# for (src_region, dst_region), df_grouped in df.groupby(["problem_src", "problem_dst"]):
#     df_grouped["throughput_speedup"] = df_grouped["throughput_achieved_gbits"] / df_grouped["baseline_throughput_achieved_gbits"]
#     df_grouped["cost_increase"] = df_grouped["cost_total"] / min(df_grouped["cost_total"])
#     df_grouped = df_grouped[df_grouped["throughput_speedup"] > 1]
#     df_grouped.sort_values(by="throughput_speedup", inplace=True)

#     x = df_grouped["throughput_speedup"]
#     y = df_grouped["cost_increase"]
#     # add (1, 1)
#     x = [1] + list(x)
#     y = [1] + list(y)
#     label = "{} to {}".format(src_region, dst_region)
#     ax.plot(x, y, label=label, alpha=0.8)
# ax.set_xlabel("Throughput speedup (x)")
# ax.set_ylabel("Cost increase (x)")
# fig.set_facecolor("white")
# st.pyplot(fig, bbox_inches="tight")

########
# Filter results to a single src, dest pair
########

st.sidebar.subheader("Filter by source region")
st.sidebar.write(f"Sources: {', '.join(src_regions_choices)}")
src_prefix = st.sidebar.text_input("Source region prefix", "")
st.sidebar.subheader("Filter by destination region")
dst_prefix = st.sidebar.text_input("Destination region prefix", "")
st.sidebar.subheader("Filter by instance limit")
instance_limit = st.sidebar.selectbox("Instance limit", instance_choices)

df = df[df["problem_src"].str.startswith(src_prefix)]
df = df[df["problem_dst"].str.startswith(dst_prefix)]
df = df[df["problem_instance_limit"] == instance_limit]
st.sidebar.info(f"Filtered to {len(df)} rows")

if len(df) > 10000:
    st.warning("Too many rows to plot. Only plotting the first 10000.")
    df = df.iloc[:10000]

########
# Plot geomean
########

########
# Plot filtered
########


st.subheader("Throughput versus cost")
fig, ax = plt.subplots(figsize=(8, 6))
for (src_region, dst_region), df_grouped in df.groupby(["problem_src", "problem_dst"]):
    df_grouped.sort_values(by="throughput_achieved_gbits", inplace=True)
    label = "{} to {}".format(src_region, dst_region)
    x = df_grouped["throughput_achieved_gbits"]
    y = df_grouped["cost_total"]
    baseline_throughput = df_grouped["baseline_throughput_achieved_gbits"].min()
    baseline_cost = min(y)
    x = [baseline_throughput] + x.tolist()
    y = [baseline_cost] + y.tolist()
    ax.plot(x, y, label=label, marker="o", linestyle="--")
if st.checkbox("Show legend (absolute)", value=True):
    ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.6), ncol=3)
ax.set_xlabel("Throughput (Gbps)")
ax.set_ylabel("Cost ($/GB)")
fig.set_facecolor("white")
st.pyplot(fig, bbox_inches="tight")

st.write("Throughput speedup versus cost increase")
fig, ax = plt.subplots(figsize=(8, 6))
for (src_region, dst_region), df_grouped in df.groupby(["problem_src", "problem_dst"]):
    df_grouped["throughput_speedup"] = df_grouped["throughput_achieved_gbits"] / df_grouped["baseline_throughput_achieved_gbits"]
    df_grouped["cost_increase"] = df_grouped["cost_total"] / min(df_grouped["cost_total"])
    df_grouped = df_grouped[df_grouped["throughput_speedup"] > 1]
    df_grouped.sort_values(by="throughput_speedup", inplace=True)

    x = df_grouped["throughput_speedup"]
    y = df_grouped["cost_increase"]
    # add (1, 1)
    x = [1] + list(x)
    y = [1] + list(y)
    label = "{} to {}".format(src_region, dst_region)
    ax.plot(x, y, label=label, alpha=0.8)
if st.checkbox("Show legend (speedup)", value=True):
    ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.6), ncol=3)
ax.set_xlabel("Throughput speedup (x)")
ax.set_ylabel("Cost increase (x)")
fig.set_facecolor("white")
st.pyplot(fig, bbox_inches="tight")


