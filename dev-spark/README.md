# Iceberg Dev Spark - Spark API for Local Table Development

Spark scripts for local Iceberg table development using a Hadoop catalog.

**Requires Spark 4.0 or 4.1 in the build (default: 4.1).**

## Scripts

### CreateTableSpark

Creates Iceberg tables, writes data, optionally enables bloom filters, and computes NDV statistics via Puffin files.

```bash
./gradlew :iceberg-dev-spark:run
```

**Arguments** (comma-separated via `-PrunArgs`):

| Arg                | Description                          | Default      |
| ------------------ | ------------------------------------ | ------------ |
| `bloom_mode`       | `none`, `row_group`, or `file_level` | `file_level` |
| `num_files`        | Number of data files to create       | `10`         |
| `records_per_file` | Rows per file                        | `100000`     |

**Examples:**

```bash
# Default: file_level bloom, 10 files × 100K rows = 1M total
./gradlew :iceberg-dev-spark:run

# Bloom mode only
./gradlew :iceberg-dev-spark:run -PrunArgs=none
./gradlew :iceberg-dev-spark:run -PrunArgs=row_group
./gradlew :iceberg-dev-spark:run -PrunArgs=file_level

# Larger dataset: 50 files × 500K rows = 25M total
./gradlew :iceberg-dev-spark:run -PrunArgs="file_level,50,500000"

# Custom size without bloom filters
./gradlew :iceberg-dev-spark:run -PrunArgs="none,20,200000"
```

Creates `local.default.sample_table_spark` with:

- Interleaved IDs across files (so min/max pruning is ineffective, but bloom filters can prune)
- Bloom filters on `id` and `data` (when enabled)
- Puffin file with NDV statistics and file-level bloom filters (when `file_level` mode)

### ReadTableSpark

Reads Iceberg tables and prints row count, schema, and a sample of rows.

```bash
./gradlew :iceberg-dev-spark:runReadTable
```

Optional: pass a table name as argument:

```bash
./gradlew :iceberg-dev-spark:runReadTable --args="local.default.sample_table_spark"
```

## Environment

| Variable            | Description                                                     |
| ------------------- | --------------------------------------------------------------- |
| `ICEBERG_WAREHOUSE` | Override warehouse location (default: `file:./build/warehouse`) |

### Run all experiments (one script)

Runs CreateTableSpark + ReadTableSpark for multiple dataset sizes and bloom modes, then writes `graphing_scripts/bloom_filter_results.json` for the plotting scripts.

**Dataset sizes** (edit `DATASET_SIZES` in `run_all_experiments.py` to add or change):

- `small`: 10 files × 100K rows = 1M total rows
- `large`: 50 files × 500K rows = 25M total rows

From repo root:

```bash
python dev-spark/run_all_experiments.py
```

Then generate graphs:

```bash
python graphing_scripts/plot_all.py
```

## Build

The dev-spark module is included when Spark 4.0 or 4.1 is in the build (4.1 by default).
