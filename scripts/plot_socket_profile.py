import argparse
import json
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt  # pytype: disable=import-error
from tqdm import tqdm

from skyplane import __root__


def plot(file):
    with open(file, "r") as f:
        data = json.load(f)["socket_profiles"]
    df = pd.DataFrame(data).sort_values("time_ms")
    print(df.columns)

    df_grouped = df.groupby("chunk_id")
    fig, ax = plt.subplots(figsize=(8, 8))
    for chunk_id, chunk_df in tqdm(df_grouped):
        ax.plot(chunk_df["time_ms"], chunk_df["bytes"], label=chunk_id, alpha=0.5)
    ax.set_xlabel("Time (ms)")
    ax.set_ylabel("Bytes transferred")
    return fig, ax


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("profile_file", help="Path to the profile file")
    parser.add_argument(
        "--plot_dir",
        default=__root__ / "data" / "figures" / "socket_profiles",
        help="Path to the directory where to save the plot",
    )
    args = parser.parse_args()

    plot_dir = Path(args.plot_dir)
    plot_dir.mkdir(parents=True, exist_ok=True)
    fig, ax = plot(args.profile_file)
    fig.savefig(plot_dir / f"{Path(args.profile_file).stem}.png", dpi=300, bbox_inches="tight")
    fig.savefig(plot_dir / f"{Path(args.profile_file).stem}.pdf", bbox_inches="tight")
