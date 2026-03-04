#!/usr/bin/env python3
"""
Run CreateTableSpark + ReadTableSpark for each bloom mode, then write
graphing_scripts/bloom_filter_results.json in the format expected by the plotting scripts.

Usage (from repo root):
  python dev-spark/run_all_experiments.py

Or from dev-spark:
  python run_all_experiments.py

Requires: dev-spark/write-metrics.json and dev-spark/read-metrics.json are produced
by each run (CreateTableSpark writes write-metrics.json, ReadTableSpark writes read-metrics.json).
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
DEV_SPARK = REPO_ROOT / "dev-spark"
GRAPHING = REPO_ROOT / "graphing_scripts"
OUTPUT_JSON = GRAPHING / "bloom_filter_results.json"

# Bloom modes: none, row_group (Parquet only), file_level (Parquet + Puffin)
BLOOM_MODES = ["none", "row_group", "file_level"]
# Dataset size label for this run (one size = one point per chart)
DATASET_SIZE = "small"


def run_gradle(task: str, run_args: Optional[str] = None) -> None:
    cmd = ["./gradlew", task]
    if run_args is not None:
        cmd.append(f"-PrunArgs={run_args}")
    print(f"  Running: {' '.join(cmd)}")
    r = subprocess.run(cmd, cwd=REPO_ROOT)
    if r.returncode != 0:
        sys.exit(r.returncode)


def load_json(path: Path) -> Dict[str, Any]:
    with path.open() as f:
        return json.load(f)


def _n(v: Any, default: float = 0) -> float:
    """Coerce value to number; use default if None or missing."""
    if v is None:
        return default
    return float(v)


def _int(v: Any, default: int = 0) -> int:
    if v is None:
        return default
    return int(v)


def build_results(experiments: List[Tuple[str, Dict[str, Any], Dict[str, Any]]]) -> Dict[str, Any]:
    """Build bloom_filter_results.json in the format expected by graphing_scripts."""
    bf_key = {
        "none": "no_bloom_filter",
        "row_group": "row_group_bloom_filter",
        "file_level": "file_level_bloom_filter",
    }
    dataset_sizes = [DATASET_SIZE]

    pruning_read = {k: [] for k in bf_key.values()}
    disk_storage_bytes = {k: [] for k in bf_key.values()}
    memory_read_mb = {k: [] for k in bf_key.values()}
    memory_write_mb = {k: [] for k in bf_key.values()}
    time_read_ms = {k: [] for k in bf_key.values()}
    time_write_ms = {k: [] for k in bf_key.values()}

    for mode, w, r in experiments:
        key = bf_key[mode]
        # Pruning: totals from write, skipped from read (use read totalRowGroups if write has none)
        total_rg = w.get("totalRowGroups") if w.get("totalRowGroups") is not None else r.get("totalRowGroups")
        pruning_read[key].append({
            "total_row_groups": _int(total_rg),
            "skipped_row_groups": _int(r.get("skippedRowGroups")),
            "total_data_files": _int(w.get("totalDataFiles")),
            "skipped_data_files": _int(r.get("skippedDataFiles")),
        })
        disk_storage_bytes[key].append({
            "puffin_bytes": _int(w.get("puffinDiskSizeInBytes")),
            "manifest_overhead_bytes": 0,
        })
        memory_read_mb[key].append(_n(r.get("maxMemoryUsage")))
        memory_write_mb[key].append(_n(w.get("maxMemoryUsage")))
        # Time (ms): durations in JSON are seconds when present
        def sec_to_ms(x):
            return round(_n(x) * 1000)
        time_read_ms[key].append({
            "metadata_ms": sec_to_ms(r.get("manifestReadDuration")),
            "puffin_ms": sec_to_ms(r.get("puffinReadDuration")),
            "data_ms": sec_to_ms(r.get("datafileReadDuration")),
        })
        time_write_ms[key].append({
            "metadata_ms": sec_to_ms(w.get("manifestWriteDuration")),
            "puffin_ms": sec_to_ms(w.get("puffinWriteDuration")),
            "data_ms": sec_to_ms(w.get("datafileWriteDuration")),
        })

    return {
        "dataset_sizes": dataset_sizes,
        "pruning_read": pruning_read,
        "disk_storage_bytes": disk_storage_bytes,
        "memory_read_mb": memory_read_mb,
        "memory_write_mb": memory_write_mb,
        "time_read_ms": time_read_ms,
        "time_write_ms": time_write_ms,
    }


def main() -> None:
    print("Running experiments (CreateTableSpark + ReadTableSpark) for each bloom mode...")
    experiments = []

    for mode in BLOOM_MODES:
        print(f"\n--- Bloom mode: {mode} ---")
        run_gradle(":iceberg-dev-spark:run", run_args=mode)
        write_path = DEV_SPARK / "write-metrics.json"
        if not write_path.exists():
            print(f"  WARNING: {write_path} not found after CreateTableSpark")
            continue
        write_metrics = load_json(write_path)

        run_gradle(":iceberg-dev-spark:runReadTable")
        read_path = DEV_SPARK / "read-metrics.json"
        if not read_path.exists():
            print(f"  WARNING: {read_path} not found after ReadTableSpark")
            continue
        read_metrics = load_json(read_path)

        experiments.append((mode, write_metrics, read_metrics))

    if not experiments:
        print("No experiments collected. Ensure write-metrics.json and read-metrics.json are produced.")
        sys.exit(1)

    results = build_results(experiments)
    GRAPHING.mkdir(parents=True, exist_ok=True)
    with OUTPUT_JSON.open("w") as f:
        json.dump(results, f, indent=2)

    print(f"\nWrote {len(experiments)} experiment(s) to {OUTPUT_JSON}")
    print("Run graphing scripts from repo root: python graphing_scripts/plot_all.py")


if __name__ == "__main__":
    main()
