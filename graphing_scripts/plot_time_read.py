"""Chart 5: Time (Read) – stacked bar: metadata + puffin + data read time."""

import argparse
import json
import os

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

from common import (
    BF_KEYS, BF_LABELS, STACK_COLORS,
    DEFAULT_DATA, DEFAULT_OUT, group_positions, save,
)


def plot(data: dict, out_dir: str):
    sizes = data["dataset_sizes"]
    offsets, ticks, _ = group_positions(len(sizes), 3)
    bar_width = 0.22

    fig, ax = plt.subplots(figsize=(10, 5))

    for i, key in enumerate(BF_KEYS):
        rows     = data["time_read_ms"][key]
        meta_s   = np.array([r["metadata_ms"] for r in rows], dtype=float) / 1000
        puffin_s = np.array([r["puffin_ms"]   for r in rows], dtype=float) / 1000
        data_s   = np.array([r["data_ms"]      for r in rows], dtype=float) / 1000

        ax.bar(offsets[i], meta_s,   bar_width, color=STACK_COLORS["metadata"])
        ax.bar(offsets[i], puffin_s, bar_width, bottom=meta_s,
               color=STACK_COLORS["puffin"])
        ax.bar(offsets[i], data_s,   bar_width, bottom=meta_s + puffin_s,
               color=STACK_COLORS["data"])

        hatch = ["", "//", "xx"][i]
        for container in ax.containers[-3:]:
            for bar in container:
                bar.set_hatch(hatch)
                bar.set_edgecolor("white")

    seg_patches = [
        mpatches.Patch(color=STACK_COLORS["metadata"], label="Planning + Metadata read"),
        mpatches.Patch(color=STACK_COLORS["puffin"],   label="Puffin file read"),
        mpatches.Patch(color=STACK_COLORS["data"],     label="Data file read"),
    ]
    hatch_patches = [
        mpatches.Patch(facecolor="lightgrey", hatch="",   edgecolor="grey", label=BF_LABELS["no_bloom_filter"]),
        mpatches.Patch(facecolor="lightgrey", hatch="//", edgecolor="grey", label=BF_LABELS["row_group_bloom_filter"]),
        mpatches.Patch(facecolor="lightgrey", hatch="xx", edgecolor="grey", label=BF_LABELS["file_level_bloom_filter"]),
    ]

    seg_legend = ax.legend(handles=seg_patches,    loc="upper left",   title="Time Components")
    ax.add_artist(seg_legend)
    ax.legend(             handles=hatch_patches,  loc="upper center", title="Bloom Filter Type")

    ax.set_xticks(ticks)
    ax.set_xticklabels(sizes)
    ax.set_xlabel("Dataset Size")
    ax.set_ylabel("Time (seconds)")
    ax.set_title("Read Time Breakdown")
    fig.tight_layout()
    save(fig, out_dir, "5_time_read.png")


def main():
    parser = argparse.ArgumentParser(description="Plot read time breakdown chart.")
    parser.add_argument("--data", default=DEFAULT_DATA)
    parser.add_argument("--out",  default=DEFAULT_OUT)
    args = parser.parse_args()
    with open(args.data) as f:
        data = json.load(f)
    os.makedirs(args.out, exist_ok=True)
    plot(data, args.out)


if __name__ == "__main__":
    main()
