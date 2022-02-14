import gc
import subprocess
import os
from pathlib import Path
import pickle
import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st
from skylark import skylark_root

data_dir = skylark_root / "data"


@st.cache(suppress_st_warning=True)
def load_data_impl(source_bucket_path, source_bucket_experiment_tag, out_dir, out_fname):
    if not out_fname.exists():
        st.info("Parsing data, this will take some time...")
        s3_path = os.path.join(source_bucket_path, source_bucket_experiment_tag)
        st.info(f"aws s3 sync {s3_path} {out_dir}")
        with st.spinner("Downloading data..."):
            subprocess.run(f"aws s3 sync {s3_path} {out_dir}", shell=True)

        dfs = []
        for file in out_dir.glob("*.pkl"):
            with st.spinner(f"Parsing {file.name}..."):
                rows = []
                with open(file, "rb") as f:
                    data_pickle = pickle.load(f)
                n_rows = len(data_pickle)
                pbar = st.progress(0)
                while data_pickle:
                    i = data_pickle.pop()
                    x = i.__dict__.copy()
                    for k, v in x["problem"].__dict__.items():
                        x[f"problem_{k}"] = v
                    del x["problem"]
                    del x["problem_const_throughput_grid_gbits"]
                    del x["problem_const_cost_per_gb_grid"]
                    del x["extra_data"]
                    del x["var_edge_flow_gigabits"]
                    del x["var_conn"]
                    del x["var_instances_per_region"]
                    del x["cost_egress_by_edge"]
                    rows.append(x)
                    pbar.progress(len(rows) / n_rows)
                del data_pickle
                dfs.append(pd.DataFrame(rows))
                st.write(f"Parsed {len(rows)} rows from {file.name}")
                del rows
                gc.collect()
        df = pd.concat(dfs)
        st.info(f"Saving data to {out_fname}, has {len(df)} rows")
        df.to_parquet(out_fname)
    else:
        df = pd.read_parquet(out_fname)

    df = df[df.is_feasible].dropna()
    df["throughput_achieved_gbits"] = df["throughput_achieved_gbits"].apply(lambda x: x[0])
    df["problem_src_region"] = df["problem_src"].apply(lambda x: x.split("-")[0])
    df["problem_dst_region"] = df["problem_dst"].apply(lambda x: x.split("-")[0])
    return df


def load_data(default_experiment_tag="2022.02.01_02.25_d20ccd26-93dd-483d-976b-91204e97c417"):
    st.sidebar.subheader(f"Select experiment")
    col1, col2, col3 = st.sidebar.columns(3)
    source_bucket_path = col1.text_input("S3 prefix", "s3://skylark-optimizer-results/pareto_data")
    source_bucket_experiment_tag = col2.text_input("Experiment tag", default_experiment_tag)
    out_dir = data_dir / "pareto_raw_data" / source_bucket_experiment_tag
    out_fname = out_dir / "pareto_data.parquet"
    out_dir.mkdir(exist_ok=True, parents=True)
    col3.button("Recompute dataframe", on_click=lambda: Path(out_fname).unlink(missing_ok=True))
    df = load_data_impl(source_bucket_path, source_bucket_experiment_tag, out_dir, out_fname).copy()
    return df, out_dir
