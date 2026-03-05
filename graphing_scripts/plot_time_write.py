"""Chart 6: Time (Write) – stacked bar: metadata + puffin + data write time."""

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

    has_other = False
    for i, key in enumerate(BF_KEYS):
        rows = data["time_write_ms"][key]
        meta_s = np.array([r.get("metadata_ms", 0) for r in rows], dtype=float) / 1000
        puffin_s = np.array([r.get("puffin_ms", 0) for r in rows], dtype=float) / 1000
        data_s = np.array([r.get("data_ms", 0) for r in rows], dtype=float) / 1000
        total_s = np.array(
            [r.get("total_ms") if r.get("total_ms") is not None else (meta_s[j] + puffin_s[j] + data_s[j]) * 1000
             for j, r in enumerate(rows)],
            dtype=float,
        ) / 1000
        stack_sum = meta_s + puffin_s + data_s
        other_s = np.maximum(0, total_s - stack_sum)
        if np.any(other_s > 0):
            has_other = True

        ax.bar(offsets[i], meta_s, bar_width, color=STACK_COLORS["metadata"])
        ax.bar(offsets[i], puffin_s, bar_width, bottom=meta_s, color=STACK_COLORS["puffin"])
        ax.bar(offsets[i], data_s, bar_width, bottom=meta_s + puffin_s, color=STACK_COLORS["data"])
        if np.any(other_s > 0):
            ax.bar(
                offsets[i], other_s, bar_width,
                bottom=meta_s + puffin_s + data_s,
                color="lightgray",
            )

        hatch = ["", "//", "xx"][i]
        for container in ax.containers[-3:]:
            for bar in container:
                bar.set_hatch(hatch)
                bar.set_edgecolor("white")

    seg_patches = [
        mpatches.Patch(color=STACK_COLORS["metadata"], label="Metadata write"),
        mpatches.Patch(color=STACK_COLORS["puffin"],   label="Puffin file write"),
        mpatches.Patch(color=STACK_COLORS["data"],     label="Data file write"),
    ]
    if has_other:
        seg_patches.append(mpatches.Patch(color="lightgray", label="Other (total write duration)"))
    hatch_patches = [
        mpatches.Patch(facecolor="lightgrey", hatch="",   edgecolor="grey", label=BF_LABELS["no_bloom_filter"]),
        mpatches.Patch(facecolor="lightgrey", hatch="//", edgecolor="grey", label=BF_LABELS["row_group_bloom_filter"]),
        mpatches.Patch(facecolor="lightgrey", hatch="xx", edgecolor="grey", label=BF_LABELS["file_level_bloom_filter"]),
    ]

    seg_legend = ax.legend(handles=seg_patches,   loc="upper left",   title="Time Components")
    ax.add_artist(seg_legend)
    ax.legend(             handles=hatch_patches, loc="upper center", title="Bloom Filter Type")

    ax.set_xticks(ticks)
    ax.set_xticklabels(sizes)
    ax.set_xlabel("Dataset Size")
    ax.set_ylabel("Time (seconds)")
    ax.set_title("Write Time Breakdown")
    fig.tight_layout()
    save(fig, out_dir, "6_time_write.png")


def main():
    parser = argparse.ArgumentParser(description="Plot write time breakdown chart.")
    parser.add_argument("--data", default=DEFAULT_DATA)
    parser.add_argument("--out",  default=DEFAULT_OUT)
    args = parser.parse_args()
    with open(args.data) as f:
        data = json.load(f)
    os.makedirs(args.out, exist_ok=True)
    plot(data, args.out)


if __name__ == "__main__":
    main()
