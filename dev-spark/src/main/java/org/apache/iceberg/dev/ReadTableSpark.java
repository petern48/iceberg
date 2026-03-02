/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use it except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.dev;

import static scala.collection.JavaConverters.mapAsJavaMapConverter;
import static scala.collection.JavaConverters.seqAsJavaListConverter;

import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.metric.SQLMetric;

/**
 * Reads Iceberg tables locally using Spark with a Hadoop catalog and demonstrates file-level bloom
 * filter pruning.
 *
 * <p>Run with: ./gradlew :iceberg-dev-spark:runReadTable
 *
 * <p>Optionally pass a table name as argument, e.g.:
 * ./gradlew :iceberg-dev-spark:runReadTable --args="local.default.sample_table_spark"
 *
 * <p>Requires Spark 4.0 or 4.1 to be in the build (default: 4.1). Tables are stored under
 * ./build/warehouse by default. Use ICEBERG_WAREHOUSE env var to override.
 */
public class ReadTableSpark {

  private static final String DEFAULT_TABLE = "local.default.sample_table_spark";

  public static void main(String[] args) throws Exception {
    String tableName = args.length > 0 ? args[0] : DEFAULT_TABLE;

    String warehouse =
        System.getenv("ICEBERG_WAREHOUSE") != null
            ? System.getenv("ICEBERG_WAREHOUSE")
            : "file:" + Paths.get("build", "warehouse").toAbsolutePath();

    SparkSession spark =
        SparkSession.builder()
            .appName("ReadTableSpark")
            .master("local[2]")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse)
            .getOrCreate();

    spark.sparkContext().setLogLevel("WARN");

    System.out.println("Reading table: " + tableName);
    System.out.println();

    // Query 1: id = 99 — value exists in exactly one file; bloom filter keeps that file,
    // prunes the other 9.
    runQuery(spark, tableName, "id = 99", "value exists in one file (expect ~1 file kept)");

    // Query 2: id = 9999 — value absent from all files; bloom filter prunes all 10 files.
    runQuery(spark, tableName, "id = 9999", "value absent from all files (expect 0 files)");

    // Query 3: id IN (1, 100) — values spread across two different files; bloom filter keeps
    // those two files, prunes the remaining 8.
    runQuery(
        spark, tableName, "id IN (1, 100)", "values in two different files (expect ~2 files kept)");

    spark.stop();
  }

  private static void runQuery(
      SparkSession spark, String tableName, String predicate, String description) {
    System.out.println("=== Query: " + predicate + " ===");
    System.out.println("    " + description);

    Dataset<Row> df = spark.table(tableName).filter(predicate);
    // Use collectAsList() — count() uses a different plan that doesn't populate scan metrics.
    List<Row> rows = df.collectAsList();
    System.out.println("  Row count: " + rows.size());

    printScanMetrics(df);
    System.out.println();
  }

  private static void printScanMetrics(Dataset<Row> df) {
    SparkPlan plan = df.queryExecution().executedPlan();
    List<SparkPlan> leaves = seqAsJavaListConverter(plan.collectLeaves()).asJava();
    if (leaves.isEmpty()) {
      System.out.println("  Metrics: (no scan in plan)");
      return;
    }

    // Find the leaf node that carries Iceberg scan metrics.
    Map<String, SQLMetric> metrics = null;
    for (SparkPlan leaf : leaves) {
      Map<String, SQLMetric> m = mapAsJavaMapConverter(leaf.metrics()).asJava();
      if (m.containsKey("totalDataFileSize") || m.containsKey("resultDataFiles")
          || m.containsKey("totalRowGroups")) {
        metrics = m;
        break;
      }
    }
    if (metrics == null) {
      metrics = mapAsJavaMapConverter(leaves.get(0).metrics()).asJava();
    }

    System.out.println("Scan metrics:");

    // Data file metrics
    printMetric(metrics, "totalScanDataFiles", "Total data files");
    printMetric(metrics, "resultDataFiles", "Result data files");
    printMetric(metrics, "skippedDataFiles", "Skipped data files");
    printMetric(metrics, "totalDataFileSize", "Total data file size (bytes)");

    // Row group metrics
    printMetric(metrics, "totalRowGroups", "Total row groups");
    printMetric(metrics, "skippedRowGroups", "Skipped row groups");

    // Puffin statistics file metrics
    printMetric(
        metrics, "puffinStatsFileSizeInBytes", "Puffin statistics file size (bytes)");
    printMetric(
        metrics,
        "puffinStatsFooterSizeInBytes",
        "Puffin statistics file footer size (bytes)");

    // Other metrics
    printMetric(metrics, "numSplits", "File splits read");
    printMetric(metrics, "numOutputRows", "Output rows");
  }

  private static void printMetric(
      Map<String, SQLMetric> metrics, String name, String description) {
    SQLMetric m = metrics.get(name);
    if (m != null) {
      System.out.println("    " + description + ": " + m.value());
    }
  }
}
