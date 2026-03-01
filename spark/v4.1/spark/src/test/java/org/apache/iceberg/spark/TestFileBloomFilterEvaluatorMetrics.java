/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.apache.iceberg.metrics.ScanMetrics;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestFileBloomFilterEvaluatorMetrics {

  private static final String STATS_PATH = "test://stats.puffin";
  private static final long SNAPSHOT_ID = 1L;
  private static final int SCHEMA_ID = 0;

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  /** No statistics file registered for the snapshot — puffin is never opened. */
  @Test
  public void noMetricsWhenNoStatisticsFile() {
    Table table = mock(Table.class);
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.snapshotId()).thenReturn(SNAPSHOT_ID);
    when(snapshot.schemaId()).thenReturn(SCHEMA_ID);
    when(table.schemas()).thenReturn(ImmutableMap.of(SCHEMA_ID, SCHEMA));
    when(table.statisticsFiles()).thenReturn(ImmutableList.of());

    ScanMetrics metrics = ScanMetrics.of(new DefaultMetricsContext());
    FileBloomFilterEvaluator result =
        FileBloomFilterEvaluator.create(
            table, snapshot, Expressions.equal("id", 42), true, metrics);

    assertThat(result).isNull();
    assertThat(metrics.puffinFilesRead().value()).isEqualTo(0L);
    assertThat(metrics.puffinReadDuration().count()).isEqualTo(0L);
  }

  /**
   * Statistics file exists but contains no bloom filter blobs for the queried column. The Puffin
   * file is still opened (1 request), so the counter and timer should both fire.
   */
  @Test
  public void metricsIncrementedWhenPuffinFileRead() throws IOException {
    InMemoryOutputFile outputFile = new InMemoryOutputFile(STATS_PATH);
    long footerSize;
    try (PuffinWriter writer = Puffin.write(outputFile).build()) {
      writer.finish();
      footerSize = writer.footerSize();
    }

    InputFile inputFile = outputFile.toInputFile();
    StatisticsFile statsFile =
        new GenericStatisticsFile(
            SNAPSHOT_ID,
            STATS_PATH,
            inputFile.getLength(),
            footerSize,
            ImmutableList.of());

    FileIO fileIO = mock(FileIO.class);
    when(fileIO.newInputFile(STATS_PATH)).thenReturn(inputFile);

    Table table = mock(Table.class);
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.snapshotId()).thenReturn(SNAPSHOT_ID);
    when(snapshot.schemaId()).thenReturn(SCHEMA_ID);
    when(table.schemas()).thenReturn(ImmutableMap.of(SCHEMA_ID, SCHEMA));
    when(table.statisticsFiles()).thenReturn(ImmutableList.of(statsFile));
    when(table.io()).thenReturn(fileIO);

    ScanMetrics metrics = ScanMetrics.of(new DefaultMetricsContext());
    // create() returns null because the Puffin file has no bloom filter blobs,
    // but the file was opened so the counter and timer must still fire.
    FileBloomFilterEvaluator result =
        FileBloomFilterEvaluator.create(
            table, snapshot, Expressions.equal("id", 42), true, metrics);

    assertThat(result).isNull();
    assertThat(metrics.puffinFilesRead().value()).isEqualTo(1L);
    assertThat(metrics.puffinReadDuration().count()).isEqualTo(1L);
    assertThat(metrics.puffinReadDuration().totalDuration().toNanos()).isGreaterThan(0L);
  }

  /**
   * Statistics file exists for a different snapshot — no match, so puffin is never opened.
   */
  @Test
  public void noMetricsWhenStatisticsFileIsForDifferentSnapshot() throws IOException {
    InMemoryOutputFile outputFile = new InMemoryOutputFile(STATS_PATH);
    long footerSize;
    try (PuffinWriter writer = Puffin.write(outputFile).build()) {
      writer.finish();
      footerSize = writer.footerSize();
    }

    long differentSnapshotId = 999L;
    StatisticsFile statsFile =
        new GenericStatisticsFile(
            differentSnapshotId,
            STATS_PATH,
            outputFile.toInputFile().getLength(),
            footerSize,
            ImmutableList.of());

    Table table = mock(Table.class);
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.snapshotId()).thenReturn(SNAPSHOT_ID);
    when(snapshot.schemaId()).thenReturn(SCHEMA_ID);
    when(table.schemas()).thenReturn(ImmutableMap.of(SCHEMA_ID, SCHEMA));
    when(table.statisticsFiles()).thenReturn(ImmutableList.of(statsFile));

    ScanMetrics metrics = ScanMetrics.of(new DefaultMetricsContext());
    FileBloomFilterEvaluator result =
        FileBloomFilterEvaluator.create(
            table, snapshot, Expressions.equal("id", 42), true, metrics);

    assertThat(result).isNull();
    assertThat(metrics.puffinFilesRead().value()).isEqualTo(0L);
    assertThat(metrics.puffinReadDuration().count()).isEqualTo(0L);
  }

  /** Reading multiple times accumulates counts across calls. */
  @Test
  public void metricsAccumulateAcrossMultipleCalls() throws IOException {
    InMemoryOutputFile outputFile = new InMemoryOutputFile(STATS_PATH);
    long footerSize;
    try (PuffinWriter writer = Puffin.write(outputFile).build()) {
      writer.finish();
      footerSize = writer.footerSize();
    }

    InputFile inputFile = outputFile.toInputFile();
    StatisticsFile statsFile =
        new GenericStatisticsFile(
            SNAPSHOT_ID,
            STATS_PATH,
            inputFile.getLength(),
            footerSize,
            ImmutableList.of());

    FileIO fileIO = mock(FileIO.class);
    when(fileIO.newInputFile(STATS_PATH)).thenReturn(inputFile);

    Table table = mock(Table.class);
    Snapshot snapshot = mock(Snapshot.class);
    when(snapshot.snapshotId()).thenReturn(SNAPSHOT_ID);
    when(snapshot.schemaId()).thenReturn(SCHEMA_ID);
    when(table.schemas()).thenReturn(ImmutableMap.of(SCHEMA_ID, SCHEMA));
    when(table.statisticsFiles()).thenReturn(ImmutableList.of(statsFile));
    when(table.io()).thenReturn(fileIO);

    ScanMetrics metrics = ScanMetrics.of(new DefaultMetricsContext());
    FileBloomFilterEvaluator.create(
        table, snapshot, Expressions.equal("id", 42), true, metrics);
    FileBloomFilterEvaluator.create(
        table, snapshot, Expressions.equal("id", 43), true, metrics);

    assertThat(metrics.puffinFilesRead().value()).isEqualTo(2L);
    assertThat(metrics.puffinReadDuration().count()).isEqualTo(2L);
  }
}
