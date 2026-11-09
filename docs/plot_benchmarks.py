"""Regenerate README charts from its published, rounded benchmark totals.

Run with matplotlib installed: python docs/plot_benchmarks.py
These are historical results, not new measurements. Query subsets are unchanged.
"""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt


OUTPUT = Path(__file__).parent / "source" / "_static" / "images"
ENGINES = ("df-dist", "Spark", "Trino")
COLORS = ("#f4c247", "#fa8e54", "#eb7ca6")
# Dataset, output name, queries compared, total seconds in ENGINES order.
RESULTS = (
    ("TPC-H SF1", "tpch_sf1", 22, (7, 30, 18)),
    ("TPC-H SF10", "tpch_sf10", 22, (10, 51, 33)),
    ("TPC-H SF100", "tpch_sf100", 19, (42, 261, 93)),
    ("TPC-DS SF1", "tpcds_sf1", 67, (29, 101, 85)),
)


def style_axes(ax):
    ax.set_axisbelow(True)
    ax.grid(axis="y", color="#e5e5e5", linewidth=0.5)
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(axis="both", labelsize=12)


def plot_totals():
    for dataset, name, queries, seconds in RESULTS:
        fig, ax = plt.subplots(figsize=(6, 4), dpi=150)
        style_axes(ax)
        bars = ax.bar(ENGINES, seconds, color=COLORS, width=0.62)
        ax.bar_label(
            bars, labels=[f"{value}s" for value in seconds], padding=4,
            fontsize=11, fontweight="bold", color="#484848",
        )
        ax.set_ylim(0, max(seconds) * 1.2)
        ax.set_ylabel("Total time (s)", fontsize=13)
        ax.set_title(
            f"{dataset}  ·  {queries} queries  ·  lower is better",
            fontsize=13, fontweight="bold",
        )
        ax.get_xticklabels()[0].set_fontweight("bold")
        fig.tight_layout()
        fig.savefig(OUTPUT / f"{name}.png")
        plt.close(fig)


def plot_relative():
    fig, ax = plt.subplots(figsize=(8, 4.8), dpi=150)
    style_axes(ax)
    width = 0.25
    for index, (engine, color) in enumerate(zip(ENGINES, COLORS)):
        values = [seconds[index] / seconds[0] for _, _, _, seconds in RESULTS]
        positions = [group + (index - 1) * width for group in range(len(RESULTS))]
        bars = ax.bar(positions, values, width=width, color=color, label=engine)
        ax.bar_label(
            bars, labels=[f"{value:.1f}×" for value in values], padding=3,
            fontsize=9, color="#484848",
        )
    ax.set_xticks(range(len(RESULTS)), [row[0] for row in RESULTS])
    ax.set_ylim(0, 7.2)
    ax.set_ylabel("Relative query time (×, lower is better)", fontsize=13)
    ax.set_title(
        "DataFusion Distributed vs other engines", fontsize=16, fontweight="bold",
    )
    ax.legend(loc="upper left", ncol=len(ENGINES), frameon=False, fontsize=11)
    fig.tight_layout()
    fig.savefig(OUTPUT / "summary_relative.png")
    plt.close(fig)


if __name__ == "__main__":
    plot_totals()
    plot_relative()
