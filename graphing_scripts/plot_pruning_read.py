"""Chart 1: Pruning (Read) – stacked bar showing skipped vs read row groups."""

import argparse
import json
import os

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

from common import (
    BF_KEYS, BF_COLORS, BF_LABELS, STACK_ALPHA_LIGHT,
    DEFAULT_DATA, DEFAULT_OUT, group_positions, save,
)


def plot(data: dict, out_dir: str, display_inline: bool = False):
    sizes = data["dataset_sizes"]
    offsets, ticks, _ = group_positions(len(sizes), 3)
    bar_width = 0.22

    fig, ax = plt.subplots(figsize=(10, 5))
    legend_handles = []

    for i, (key, color) in enumerate(zip(BF_KEYS, BF_COLORS)):
        rows    = data["pruning_read"][key]
        totals  = np.array([r["total_row_groups"]  for r in rows], dtype=float)
        skipped = np.array([r["skipped_row_groups"] for r in rows], dtype=float)
        read_rg = totals - skipped

        ax.bar(offsets[i], skipped, bar_width, color=color)
        ax.bar(offsets[i], read_rg, bar_width, bottom=skipped,
               color=color, alpha=STACK_ALPHA_LIGHT)
        legend_handles.append(mpatches.Patch(color=color, label=BF_LABELS[key]))

    skip_patch = mpatches.Patch(facecolor="dimgray",                       label="Skipped (pruned)")
    read_patch = mpatches.Patch(facecolor="dimgray", alpha=STACK_ALPHA_LIGHT, label="Read (not pruned)")

    color_legend = ax.legend(handles=legend_handles,         loc="upper left",   title="Bloom Filter Type")
    ax.add_artist(color_legend)
    ax.legend(         handles=[skip_patch, read_patch], loc="upper center", title="Bar Segments")

    ax.set_xticks(ticks)
    ax.set_xticklabels(sizes)
    ax.set_xlabel("Dataset Size")
    ax.set_ylabel("Row Groups")
    ax.set_title("Pruning – Row Groups Read vs Skipped (Read Path)")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
    fig.tight_layout()
    save(fig, out_dir, "1_pruning_read.png", display_inline=display_inline)


def main():
    parser = argparse.ArgumentParser(description="Plot pruning (read) chart.")
    parser.add_argument("--data", default=DEFAULT_DATA)
    parser.add_argument("--out",  default=DEFAULT_OUT)
    args = parser.parse_args()
    with open(args.data) as f:
        data = json.load(f)
    os.makedirs(args.out, exist_ok=True)
    plot(data, args.out)


if __name__ == "__main__":
    main()
