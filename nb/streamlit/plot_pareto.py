import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from tqdm.notebook import tqdm
import seaborn as sns
import streamlit as st
from skylark import skylark_root

data_dir = skylark_root / "data"
figure_dir = data_dir / "figures" / "pareto_speedups_updated"
figure_dir.mkdir(exist_ok=True, parents=True)

plt.style.use("seaborn-bright")
plt.set_cmap("plasma")

@st.cache
def load_df():
    df = pd.read_parquet(data_dir / "df_sweep.parquet")
    df = df.drop(columns=["problem_const_throughput_grid_gbits", "problem_const_cost_per_gb_grid"]).dropna()
    df['throughput_achieved_gbits'] = df['throughput_achieved_gbits'].apply(lambda x: x[0])
    df["problem_src_region"] = df["problem_src"].apply(lambda x: x.split("-")[0])
    df["problem_dst_region"] = df["problem_dst"].apply(lambda x: x.split("-")[0])
    return df

df = load_df()

# select rows where throughput_achieved_gbits >= baseline_throughput_gbits
df = df[df["throughput_achieved_gbits"] >= df["baseline_throughput_gbits"]]

src_regions_choices = sorted(df["problem_src"].unique())
dst_regions_choices = sorted(df["problem_dst_region"].unique())
instance_choices = sorted(df["problem_instance_limit"].unique())

# select src, dst, instance limit
src_prefix = st.sidebar.text_input("Source region prefix", "aws:us-east-1")
dst_prefix = st.sidebar.text_input("Destination region prefix", "aws:")
instance_limit = st.sidebar.selectbox("Instance limit", instance_choices)

df = df[(df["problem_src"].str.startswith(src_prefix)) & (df["problem_dst"].str.startswith(dst_prefix)) & (df["problem_instance_limit"] == instance_limit)]


if len(df) > 1000:
    st.warning("Too many rows to plot. Only plotting the first 1000.")
    df = df.iloc[:1000]

st.write("Throughput versus cost")
fig, ax = plt.subplots(figsize=(8, 6))
for (src_region, dst_region), df_grouped in df.groupby(["problem_src", "problem_dst"]):
    df_grouped.sort_values(by="throughput_achieved_gbits", inplace=True)
    label = "{} to {}".format(src_region, dst_region)
    x = df_grouped["throughput_achieved_gbits"]
    y = df_grouped["cost_total"]
    baseline_throughput = df_grouped["baseline_throughput_gbits"].min()
    baseline_cost = min(y)
    x = [baseline_throughput] + x.tolist()
    y = [baseline_cost] + y.tolist()
    ax.plot(x, y, label=label)
ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.6), ncol=3)
ax.set_xlabel("Throughput (Gbps)")
ax.set_ylabel("Cost ($/GB)")
fig.set_facecolor("white")
st.pyplot(fig, bbox_inches="tight")

st.write("Throughput speedup versus cost increase")
fig, ax = plt.subplots(figsize=(8, 6))
for (src_region, dst_region), df_grouped in df.groupby(["problem_src", "problem_dst"]):
    df_grouped["throughput_speedup"] = df_grouped["throughput_achieved_gbits"] / df_grouped["baseline_throughput_gbits"]
    df_grouped["cost_increase"] = df_grouped["cost_total"] / min(df_grouped["cost_total"])
    df_grouped.sort_values(by="throughput_speedup", inplace=True)

    x = df_grouped["throughput_speedup"]
    y = df_grouped["cost_increase"]
    # add (1, 1)
    x = [1] + list(x)
    y = [1] + list(y)
    label = "{} to {}".format(src_region, dst_region)
    ax.plot(x, y, label=label, alpha=0.8)
ax.legend(loc="lower center", bbox_to_anchor=(0.5, -0.6), ncol=3)
ax.set_xlabel("Throughput speedup (x)")
ax.set_ylabel("Cost increase (x)")
fig.set_facecolor("white")
st.pyplot(fig, bbox_inches="tight")

# show df
st.write("Dataframe")
st.write(df)