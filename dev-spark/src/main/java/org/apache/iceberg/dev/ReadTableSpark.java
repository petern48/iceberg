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

// For writing metrics to a json file
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.metric.SQLMetric;

import org.apache.iceberg.dev.ReadMetrics;

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
    MemoryTracker.Result mem1 =
        runQueryWithMemoryTracking(
            spark, tableName, "id = 99", "value exists in one file (expect ~1 file kept)");

    // Query 2: id = 9999 — value absent from all files; bloom filter prunes all 10 files.
    MemoryTracker.Result mem2 =
        runQueryWithMemoryTracking(
            spark, tableName, "id = 9999", "value absent from all files (expect 0 files)");

    // Query 3: id IN (1, 100) — values spread across two different files; bloom filter keeps
    // those two files, prunes the remaining 8.
    MemoryTracker.Result mem3 =
        runQueryWithMemoryTracking(
            spark,
            tableName,
            "id IN (1, 100)",
            "values in two different files (expect ~2 files kept)");

    // Print memory summary
    System.out.println("=== Memory Summary ===");
    System.out.println("  Query 1 (id = 99): " + mem1);
    System.out.println("  Query 2 (id = 9999): " + mem2);
    System.out.println("  Query 3 (id IN (1, 100)): " + mem3);
    System.out.println();

    // Export fake data .json data for development purposes
    ReadMetrics metrics = new ReadMetrics();
    metrics.skippedRowGroups = 10;
    metrics.skippedDataFiles = 3;
    metrics.maxMemoryUsage = (float) mem3.peakMemoryMB();
    metrics.totalReadDuration = (float) mem3.durationMs();
    metrics.puffinReadDuration = 12.3f;
    metrics.manifestReadDuration = 5.7f;
    metrics.datafileReadDuration = 20.1f;

    exportReadMetrics(metrics);

    spark.stop();
  }

  private static void exportReadMetrics(ReadMetrics metrics) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);

    // TODO: parametrize the file name using input arguments
    // This should be write a file at spark-dev/read-metrics.json
    String outputPath = "read-metrics.json";
    mapper.writeValue(Paths.get(outputPath).toFile(), metrics);
    System.out.println("Read Metrics exported to " + outputPath);
  }

  private static MemoryTracker.Result runQueryWithMemoryTracking(
      SparkSession spark, String tableName, String predicate, String description) {
    System.out.println("=== Query: " + predicate + " ===");
    System.out.println("    " + description);

    Dataset<Row> df = spark.table(tableName).filter(predicate);

    // Track memory and duration while executing the query
    MemoryTracker.TrackedResult<List<Row>> tracked =
        MemoryTracker.trackWithResult(df::collectAsList);

    List<Row> rows = tracked.value();
    MemoryTracker.Result memResult = tracked.metrics();

    System.out.println("  Row count: " + rows.size());
    System.out.println(
        "  Peak memory: " + String.format(Locale.ROOT, "%.2f MB", memResult.peakMemoryMB()));
    System.out.println(
        "  Duration: " + String.format(Locale.ROOT, "%.2f ms", memResult.durationMs()));

    printScanMetrics(df);
    System.out.println();

    return memResult;
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
    } else {
      System.out.println("    Metric " + name + " NOT FOUND");
    }
  }
}
