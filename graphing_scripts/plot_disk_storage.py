"""Chart 2: Disk Storage – stacked bar showing puffin bytes + manifest overhead."""

import argparse
import json
import os

import matplotlib.patches as mpatches
import numpy as np

from common import (
    BF_KEYS, BF_COLORS, BF_LABELS, STACK_ALPHA_LIGHT,
    DEFAULT_DATA, DEFAULT_OUT, group_positions, save,
)

import matplotlib.pyplot as plt


def _bytes_to_mb(b: float) -> float:
    return b / (1024 ** 2)


def plot(data: dict, out_dir: str, display_inline: bool = False):
    sizes = data["dataset_sizes"]
    offsets, ticks, _ = group_positions(len(sizes), 3)
    bar_width = 0.22

    fig, ax = plt.subplots(figsize=(10, 5))
    legend_handles = []

    for i, (key, color) in enumerate(zip(BF_KEYS, BF_COLORS)):
        rows     = data["disk_storage_bytes"][key]
        puffin   = np.array([_bytes_to_mb(r["puffin_bytes"])            for r in rows])
        manifest = np.array([_bytes_to_mb(r["manifest_overhead_bytes"]) for r in rows])

        ax.bar(offsets[i], puffin,   bar_width, color=color)
        ax.bar(offsets[i], manifest, bar_width, bottom=puffin,
               color=color, alpha=STACK_ALPHA_LIGHT)
        legend_handles.append(mpatches.Patch(color=color, label=BF_LABELS[key]))

    puffin_patch   = mpatches.Patch(facecolor="dimgray",                       label="Puffin file storage")
    manifest_patch = mpatches.Patch(facecolor="dimgray", alpha=STACK_ALPHA_LIGHT, label="Manifest overhead")

    color_legend = ax.legend(handles=legend_handles,               loc="upper left",   title="Bloom Filter Type")
    ax.add_artist(color_legend)
    ax.legend(         handles=[puffin_patch, manifest_patch], loc="upper center", title="Bar Segments")

    ax.set_xticks(ticks)
    ax.set_xticklabels(sizes)
    ax.set_xlabel("Dataset Size")
    ax.set_ylabel("Additional Disk Usage (MB)")
    ax.set_title("Disk Storage Added by Bloom Filters")
    fig.tight_layout()
    save(fig, out_dir, "2_disk_storage.png", display_inline=display_inline)


def main():
    parser = argparse.ArgumentParser(description="Plot disk storage chart.")
    parser.add_argument("--data", default=DEFAULT_DATA)
    parser.add_argument("--out",  default=DEFAULT_OUT)
    args = parser.parse_args()
    with open(args.data) as f:
        data = json.load(f)
    os.makedirs(args.out, exist_ok=True)
    plot(data, args.out)


if __name__ == "__main__":
    main()
