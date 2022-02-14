from tkinter import font
from typing import Dict
import matplotlib.pyplot as plt
from questionary import checkbox
import streamlit as st
from nb.streamlit.common import load_data
from skylark import skylark_root
import numpy as np

data_dir = skylark_root / "data"
figure_dir = data_dir / "figures" / "pareto_speedups_updated"
figure_dir.mkdir(exist_ok=True, parents=True)

avail_style = list(plt.style.available)
avail_cmap = list(plt.colormaps())
col1, col2 = st.sidebar.columns(2)
style = col1.selectbox("Style", avail_style, avail_style.index("seaborn-bright"))
cmap = col2.selectbox("Color map", avail_cmap, avail_cmap.index("plasma"))
plt.set_cmap(cmap)
# bold axis labels w/ font weight
plt.rcParams["axes.labelweight"] = "bold"

# df.columns = [
#   "is_feasible",
#   "throughput_achieved_gbits",
#   "cost_egress",
#   "cost_instance",
#   "cost_total",
#   "transfer_runtime_s",
#   "baseline_throughput_achieved_gbits",
#   "baseline_cost_egress",
#   "baseline_cost_instance",
#   "baseline_cost_total",
#   "problem_src",
#   "problem_dst",
#   "problem_required_throughput_gbits",
#   "problem_gbyte_to_transfer",
#   "problem_instance_limit",
#   "problem_aws_instance_throughput_limit",
#   "problem_gcp_instance_throughput_limit",
#   "problem_azure_instance_throughput_limit",
#   "problem_src_region",
#   "problem_dst_region"
# ]

with st.spinner("Loading data"):
    df, out_dir = load_data()
    df["throughput_speedup"] = df["throughput_achieved_gbits"] / df["baseline_throughput_achieved_gbits"]
    df["cost_increase"] = df["cost_total"] / df["baseline_cost_total"]
    df["src_provider"] = df["problem_src"].apply(lambda x: x.split(":")[0])
    df["dst_provider"] = df["problem_dst"].apply(lambda x: x.split(":")[0])

cost_threshold = st.slider("Cost threshold", 1.0, 3.0, 1.25, 0.05)
ignore_speedup_one = st.checkbox("Ignore speedup 1.0", value=False)

st.sidebar.subheader("Plot configuration")
col1, col2 = st.sidebar.columns(2)
log_x = col1.checkbox("Log x-axis")
log_y = col2.checkbox("Log y-axis")
col1, col2 = st.sidebar.columns(2)
xmax = col1.slider("X-axis max", 1.0, 10.0, 5.0, 0.5)
bins = col2.slider("Histogram Bins", 10, 100, 15, 5)
col1, col2 = st.sidebar.columns(2)
plot_width = col1.slider("Plot width", 1.0, 20.0, 7.5, 0.25)
plot_height = col2.slider("Plot height", 1.0, 20.0, 1.75, 0.25)

src_regions_choices = sorted(df["problem_src"].unique())
dst_regions_choices = sorted(df["problem_dst"].unique())
instance_choices = sorted(df["problem_instance_limit"].unique())

st.sidebar.subheader("Filters")
col1, col2 = st.sidebar.columns(2)
src_prefix = col1.text_input("Source region prefix", "")
dst_prefix = col2.text_input("Dest. region prefix", "")
instance_limit = st.sidebar.selectbox("Instance limit", instance_choices)

df = df[df["problem_src"].str.startswith(src_prefix)]
df = df[df["problem_dst"].str.startswith(dst_prefix)]
df = df[df["problem_instance_limit"] == instance_limit]
st.sidebar.info(f"Filtered to {len(df)} rows")


label_map = {
    "aws": "AWS",
    "gcp": "GCP",
    "azure": "Azure",
}


def geomean(x):
    return np.exp(np.mean(np.log(x)))


st.write(f"Geometric mean throughput speedup: {geomean(df['throughput_speedup'].values):.2f}")

geomeans = {}
geomeans_dst = {}
for src_region, df_src in df.groupby("problem_src"):
    geomeans[src_region] = geomean(df_src["throughput_speedup"].values)
for dst_region, df_dst in df.groupby("problem_dst"):
    geomeans_dst[dst_region] = geomean(df_dst["throughput_speedup"].values)

if st.checkbox("Enable plots", True):
    # st.header("Sorted geomean by source")
    # with plt.style.context(style):
    #     fig, axs = plt.subplots(1, 3, sharex=True, sharey=True, figsize=(plot_width, plot_height))
    #     for i, (src_provider, src_df) in enumerate(df.groupby('src_provider')):
    #         # get top 5 source regions for provider in geomeans
    #         src_geomeans = sorted([(k, v) for k, v in geomeans.items() if k.startswith(src_provider)], key=lambda i: i[1], reverse=True)
    #         # plot bar plot with for top 5 source regions
    #         src_geomeans: Dict[str, float] = dict(src_geomeans[:5])
    #         axs[i].bar(range(len(src_geomeans)), [v for k, v in src_geomeans.items()], align='center')
    #         axs[i].set_title(label_map[src_provider])
    #     st.pyplot(fig)

    # st.header("Source to Destination grouped histogram")
    # with plt.style.context(style):
    #     fig, axs = plt.subplots(3, 3, sharex=True, sharey=True, figsize=(plot_width, plot_height * 3))
    #     for ((src_provider, dst_provider), df_group), ax in zip(df.groupby(["src_provider", "dst_provider"]), axs.flatten()):
    #         if ignore_speedup_one:
    #             df_group = df_group[df_group["throughput_speedup"] >= 1.01]
    #         max_speedup = df_group.query("cost_increase < @cost_threshold").groupby(["problem_src", "problem_dst"])["throughput_speedup"].max().sort_values(ascending=False)
    #         max_speedup = max_speedup.clip(lower=1, upper=xmax)
    #         ax.hist(max_speedup, bins=np.linspace(1, xmax, bins), histtype="stepfilled")
    #         ax.set_title(f"{label_map[src_provider]} to {label_map[dst_provider]}")
    #         ax.set_xlim(1, xmax)
    #         ax.set_xscale("log" if log_x else "linear")
    #         ax.set_yscale("log" if log_y else "linear")
    #     axs[2][1].set_xlabel("Throughput speedup")
    #     axs[1][0].set_ylabel("Count")
    #     fig.set_facecolor("white")
    #     st.pyplot(fig, bbox_inches="tight")
    #     f = f"src_to_dst_histogram_{cost_threshold:.2f}.pdf"
    #     fig.savefig(str(out_dir / f), dpi=300, bbox_inches="tight")
    #     st.download_button("Download PDF: " + f, (out_dir / f).read_bytes(), file_name=f)

    # st.header("Source grouped histogram")
    # with plt.style.context(style):
    #     fig, axs = plt.subplots(1, 3, sharex=True, sharey=True, figsize=(plot_width, plot_height))
    #     for (src_provider, df_group), ax in zip(df.groupby('src_provider'), axs):
    #         if ignore_speedup_one:
    #             df_group = df_group[df_group["throughput_speedup"] >= 1.01]
    #         max_speedup = df_group.query("cost_increase < @cost_threshold").groupby(["problem_src", "problem_dst"])["throughput_speedup"].max().sort_values(ascending=False)
    #         max_speedup = max_speedup.clip(lower=1, upper=xmax)  # set range of plot
    #         ax.hist(max_speedup, bins=np.linspace(1, xmax, bins), histtype="stepfilled")
    #         ax.set_title(f"Source: {label_map[src_provider]}")
    #         ax.set_xlim(1, xmax)
    #         ax.set_xscale("log" if log_x else "linear")
    #         ax.set_yscale("log" if log_y else "linear")
    #     axs[1].set_xlabel("Throughput speedup")
    #     axs[0].set_ylabel("Count")
    #     fig.set_facecolor("white")
    #     st.pyplot(fig, bbox_inches="tight")
    #     f = f"src_histogram_{cost_threshold:.2f}.pdf"
    #     fig.savefig(str(out_dir / f), dpi=300, bbox_inches="tight")
    #     st.download_button("Download PDF: " + f, (out_dir / f).read_bytes(), file_name=f)

    # st.header("Destination grouped histogram")
    # with plt.style.context(style):
    #     fig, axs = plt.subplots(1, 3, sharex=True, sharey=True, figsize=(plot_width, plot_height))
    #     for (dst_provider, df_group), ax in zip(df.groupby('dst_provider'), axs):
    #         if ignore_speedup_one:
    #             df_group = df_group[df_group["throughput_speedup"] >= 1.01]
    #         max_speedup = df_group.query("cost_increase < @cost_threshold").groupby(["problem_src", "problem_dst"])["throughput_speedup"].max().sort_values(ascending=False)
    #         max_speedup = max_speedup.clip(lower=1, upper=xmax)  # set range of plot
    #         ax.hist(max_speedup, bins=np.linspace(1, xmax, bins), histtype="stepfilled")
    #         ax.set_title(f"Destination: {label_map[dst_provider]}")
    #         ax.set_xlim(1, xmax)
    #         ax.set_xscale("log" if log_x else "linear")
    #         ax.set_yscale("log" if log_y else "linear")
    #     axs[1].set_xlabel("Throughput speedup")
    #     axs[0].set_ylabel("Count")
    #     fig.set_facecolor("white")
    #     st.pyplot(fig, bbox_inches="tight")
    #     f = f"dst_histogram_{cost_threshold:.2f}.pdf"
    #     fig.savefig(str(out_dir / f), dpi=300, bbox_inches="tight")
    #     st.download_button("Download PDF: " + f, (out_dir / f).read_bytes(), file_name=f)

    st.header("Distribution of all speedups at different cost thresholds")
    cost_threshold_ranges = [1.10, 1.25, 1.5, 2.0]
    col1, col2, col3, col4, col5 = st.columns(5)
    whis = col1.number_input("whis", 1.0, 100.0, 4.0, 0.25)
    max_flier = col2.text_input("max_flier", ".99")
    mode = col3.radio("mode", ["intracloud", "intercloud", "both"], 2)
    providers = ["aws", "azure", "gcp"]
    src_providers = col4.multiselect("Source cloud:", providers, providers)
    dst_providers = col5.multiselect("Dest cloud:", providers, providers)
    with plt.style.context(style):
        fig, axs = plt.subplots(2, 1, figsize=(plot_height * 2, plot_height * 2))
        for ax, outliers in zip(axs, [False, True]):
            for i, thresh in enumerate(cost_threshold_ranges):
                df_group = df.query("cost_increase <= @thresh")
                df_group = df_group[df_group["throughput_speedup"] >= 1.001]

                if mode == "intracloud":
                    df_group = df_group[df_group["src_provider"] == df_group["dst_provider"]]
                elif mode == "intercloud":
                    df_group = df_group[df_group["src_provider"] != df_group["dst_provider"]]

                df_group = df_group[df_group["src_provider"].isin(src_providers)]
                df_group = df_group[df_group["dst_provider"].isin(dst_providers)]

                if outliers is False:
                    df_group = df_group[df_group["throughput_speedup"] <= df_group.throughput_speedup.quantile(float(max_flier))]
                max_speedup = (
                    df_group.groupby(["problem_src", "problem_dst"])["throughput_speedup"].max().sort_values(ascending=False).clip(lower=1)
                )
                label = f"{thresh:.2f}x"
                ax.boxplot(
                    max_speedup,
                    notch=False,
                    vert=False,
                    whis=whis,
                    positions=[i],
                    labels=[label],
                    widths=0.5,
                    showfliers=outliers,
                )
        axs[0].set_xlabel("Throughput speedup (excluding top 0.5%)", fontsize=8)
        axs[1].set_xlabel("Throughput speedup (all destination pairs)", fontsize=8)
        axs[0].set_ylabel("Cost increase", fontsize=8)
        axs[1].set_ylabel("Cost increase", fontsize=8)
        fig.set_facecolor("white")
        fig.tight_layout()
        st.pyplot(fig, bbox_inches="tight")
        f = f"all_speedups_boxplot_{float(max_flier):.2f}_outliers{outliers}.pdf"
        fig.savefig(str(out_dir / f), dpi=300, bbox_inches="tight")
        st.download_button("Download PDF: " + f, (out_dir / f).read_bytes(), file_name=f)

cols = st.multiselect("Columns", df.columns, ["throughput_speedup", "throughput_achieved_gbits", "baseline_throughput_achieved_gbits"])
max_speedup = (
    df.query("cost_increase < @cost_threshold")
    .groupby(["problem_src", "problem_dst"])[cols]
    .max()
    .sort_values(ascending=False, by="throughput_speedup")
)
st.dataframe(max_speedup.head(20))
