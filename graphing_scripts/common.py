"""Shared constants and helpers for bloom filter evaluation graphs."""

import os

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

# ---------------------------------------------------------------------------
# Colour palette
# ---------------------------------------------------------------------------
COLORS = {
    "no_bf":   "#4c72b0",  # muted blue
    "rg_bf":   "#dd8452",  # muted orange
    "file_bf": "#55a868",  # muted green
}

BF_LABELS = {
    "no_bloom_filter":         "No Bloom Filter",
    "row_group_bloom_filter":  "Row-Group BF",
    "file_level_bloom_filter": "File-Level BF",
}

BF_KEYS   = list(BF_LABELS.keys())
BF_COLORS = [COLORS["no_bf"], COLORS["rg_bf"], COLORS["file_bf"]]

STACK_ALPHA_LIGHT = 0.45  # lighter shade for the "active/read" portion of stacked bars

STACK_COLORS = {
    "metadata": "#8da0cb",  # periwinkle
    "puffin":   "#fc8d62",  # coral
    "data":     "#66c2a5",  # teal
}

_HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA = os.path.join(_HERE, "bloom_filter_results.json")
DEFAULT_OUT  = os.path.join(_HERE, "graphs")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def group_positions(n_groups: int, n_bars: int, bar_width: float = 0.22, gap: float = 0.08):
    """Return (offsets, tick_centers, group_width) for a grouped bar chart."""
    group_width = n_bars * bar_width + gap
    group_starts = np.arange(n_groups) * group_width
    offsets = [group_starts + i * bar_width for i in range(n_bars)]
    tick_centers = group_starts + (n_bars * bar_width) / 2
    return offsets, tick_centers, group_width


def save(fig, out_dir: str, name: str):
    path = os.path.join(out_dir, name)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    print(f"  Saved {path}")
    plt.close(fig)


def bf_color_legend_handles():
    return [mpatches.Patch(color=c, label=BF_LABELS[k]) for k, c in zip(BF_KEYS, BF_COLORS)]
