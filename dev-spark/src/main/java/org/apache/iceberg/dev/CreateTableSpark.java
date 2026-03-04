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

// For writing metrics to a json file
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.util.Locale;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ComputeTableStats;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.actions.SparkActions;

import org.apache.iceberg.dev.WriteMetrics;

/**
 * Spark equivalent of CreateTable.java - creates Iceberg tables locally using Spark APIs with a
 * Hadoop catalog, writes data, enables bloom filters, and computes NDV statistics.
 *
 * <p>Run with: ./gradlew :iceberg-dev-spark:run
 * <p>Optional args: [bloom_mode] — one of "none", "row_group", "file_level" (default: "file_level").
 *
 * <p>Requires Spark 4.0 or 4.1 to be in the build (default: 4.1). Tables are stored under
 * ./build/warehouse by default. Use ICEBERG_WAREHOUSE env var to override.
 */
public class CreateTableSpark {

  /** Bloom mode: none, row_group (Parquet only), file_level (Parquet + Puffin file-level). */
  public static String bloomModeFromArgs(String[] args) {
    if (args == null || args.length == 0) {
      return "file_level";
    }
    String mode = args[0].trim().toLowerCase(Locale.ROOT);
    return mode.isEmpty() ? "file_level" : mode;
  }

  /** TBLPROPERTIES fragment for CREATE TABLE (no leading/trailing comma). */
  public static String tblPropertiesForBloomMode(String bloomMode) {
    switch (bloomMode) {
      case "none":
        return "";
      case "row_group":
        return "TBLPROPERTIES ("
            + "'write.parquet.bloom-filter-enabled.column.id'='true',"
            + "'write.parquet.bloom-filter-enabled.column.data'='true'"
            + ")";
      case "file_level":
      default:
        return "TBLPROPERTIES ("
            + "'write.parquet.bloom-filter-enabled.column.id'='true',"
            + "'write.parquet.bloom-filter-enabled.column.data'='true',"
            + "'write.puffin.bloom-filter-enabled.column.id'='true'"
            + ")";
    }
  }

  public static void main(String[] args) throws Exception {
    String bloomMode = bloomModeFromArgs(args);

    String warehouse =
        System.getenv("ICEBERG_WAREHOUSE") != null
            ? System.getenv("ICEBERG_WAREHOUSE")
            : "file:" + Paths.get("build", "warehouse").toAbsolutePath();

    SparkSession spark =
        SparkSession.builder()
            .appName("CreateTableSpark")
            .master("local[2]")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", warehouse)
            .getOrCreate();

    spark.sparkContext().setLogLevel("WARN");

    String tableName = "local.default.sample_table_spark";

    spark.sql("USE local");
    spark.sql("CREATE NAMESPACE IF NOT EXISTS default");
    spark.sql("DROP TABLE IF EXISTS " + tableName);

    String tblProps = tblPropertiesForBloomMode(bloomMode);
    String createSql =
        "CREATE TABLE "
            + tableName
            + " (id BIGINT, data STRING, created_at TIMESTAMP) "
            + "USING iceberg "
            + (tblProps.isEmpty() ? "" : " " + tblProps);
    spark.sql(createSql);

    System.out.println("Created table: " + tableName);
    System.out.println("Bloom mode: " + bloomMode);

    int numDataFiles = 10;
    int recordsPerFile = 10;
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < numDataFiles * recordsPerFile; i++) {
      rows.add(
          org.apache.spark.sql.RowFactory.create(
              (long) (i + 1),
              "item_" + (i + 1),
              java.sql.Timestamp.from(
                  java.time.OffsetDateTime.parse("2024-01-15T10:00:00Z").plusDays(i).toInstant())));
    }

    StructType schema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.LongType, false),
              DataTypes.createStructField("data", DataTypes.StringType, false),
              DataTypes.createStructField("created_at", DataTypes.TimestampType, true)
            });

    Dataset<Row> df = spark.createDataFrame(rows, schema);
    df.repartition(numDataFiles).writeTo(tableName).append();

    System.out.println("Wrote " + (numDataFiles * recordsPerFile) + " records in " + numDataFiles + " data files");

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    table.refresh();

    ComputeTableStats.Result result =
        SparkActions.get().computeTableStats(table).columns("id", "data").execute();

    long ndvBlobs = result.statisticsFile().blobMetadata().stream()
        .filter(m -> m.properties().containsKey("ndv"))
        .count();
    long bloomBlobs = result.statisticsFile().blobMetadata().stream()
        .filter(m -> m.properties().containsKey("data-file-path"))
        .count();
    System.out.println("Puffin stats file: " + result.statisticsFile().path());
    System.out.println("  NDV blobs:               " + ndvBlobs);
    System.out.println("  File bloom filter blobs: " + bloomBlobs);
    if (bloomBlobs > 0) {
      System.out.println("  Sample bloom filter files:");
      result.statisticsFile().blobMetadata().stream()
          .filter(m -> m.properties().containsKey("data-file-path"))
          .limit(3)
          .forEach(m -> System.out.println("    " + m.properties().get("data-file-path")));
    }

    WriteMetrics metrics = new WriteMetrics();
    metrics.totalDataFiles = numDataFiles;
    metrics.puffinDiskSizeInBytes = result.statisticsFile().fileSizeInBytes();
    metrics.puffinFooterSizeInBytes = result.statisticsFile().fileFooterSizeInBytes();
    // totalRowGroups, maxMemoryUsage, *Duration — not available from write path; leave null

    exportWriteMetrics(metrics);

    spark.stop();
  }

  private static void exportWriteMetrics(WriteMetrics metrics) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    String outputPath = "write-metrics.json";
    mapper.writeValue(Paths.get(outputPath).toFile(), metrics);
    System.out.println("Write metrics exported to " + outputPath);
  }
}
