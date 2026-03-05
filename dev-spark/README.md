# Iceberg Dev Spark - Spark API for Local Table Development

Spark scripts for local Iceberg table development using a Hadoop catalog.

**Requires Spark 4.0 or 4.1 in the build (default: 4.1).**

## Scripts

### CreateTableSpark

Creates Iceberg tables, writes data, optionally enables bloom filters, and computes NDV statistics via Puffin files.

```bash
./gradlew :iceberg-dev-spark:run
```

Optional bloom mode (first arg): `none`, `row_group`, or `file_level` (default).

```bash
./gradlew :iceberg-dev-spark:run -PrunArgs=none
./gradlew :iceberg-dev-spark:run -PrunArgs=row_group
./gradlew :iceberg-dev-spark:run -PrunArgs=file_level
```

Creates `local.default.sample_table_spark` with:
- 100 records in 10 data files
- Bloom filters on `id` and `data`
- Puffin file with NDV for `data` column

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

| Variable | Description |
|----------|-------------|
| `ICEBERG_WAREHOUSE` | Override warehouse location (default: `file:./build/warehouse`) |

### Run all experiments (one script)

Runs CreateTableSpark + ReadTableSpark for each bloom mode (`none`, `row_group`, `file_level`), then writes `graphing_scripts/bloom_filter_results.json` for the plotting scripts.

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
