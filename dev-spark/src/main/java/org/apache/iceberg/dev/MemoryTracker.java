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
package org.apache.iceberg.dev;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Utility class for tracking peak memory usage and duration during operations.
 *
 * <p>Uses a background polling thread to capture peak memory usage, which is more accurate than
 * simple before/after measurement since it captures spikes that may be garbage collected before the
 * operation completes.
 */
public class MemoryTracker {

  private static final int POLL_INTERVAL_MS = 5;

  public static class Result {
    private final long peakMemoryBytes;
    private final long durationNanos;

    Result(long peakMemoryBytes, long durationNanos) {
      this.peakMemoryBytes = peakMemoryBytes;
      this.durationNanos = durationNanos;
    }

    public long peakMemoryBytes() {
      return peakMemoryBytes;
    }

    public double peakMemoryMB() {
      return peakMemoryBytes / (1024.0 * 1024.0);
    }

    public long durationNanos() {
      return durationNanos;
    }

    public double durationMs() {
      return durationNanos / 1_000_000.0;
    }

    @Override
    public String toString() {
      return String.format(Locale.ROOT, "%.2f MB, %.2f ms", peakMemoryMB(), durationMs());
    }
  }

  /**
   * Tracks peak memory usage and duration while running the given operation.
   *
   * @param operation the operation to run and measure
   * @return the measurement result containing peak memory and duration
   */
  public static Result track(Runnable operation) {
    Runtime runtime = Runtime.getRuntime();
    AtomicLong peakMemory = new AtomicLong(0);
    AtomicBoolean running = new AtomicBoolean(true);

    Thread monitor =
        new Thread(
            () -> {
              while (running.get()) {
                long used = runtime.totalMemory() - runtime.freeMemory();
                peakMemory.updateAndGet(current -> Math.max(current, used));
                try {
                  Thread.sleep(POLL_INTERVAL_MS);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  break;
                }
              }
            });
    monitor.setDaemon(true);

    long startTime = System.nanoTime();
    monitor.start();

    try {
      operation.run();
    } finally {
      running.set(false);
      try {
        monitor.join(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    long endTime = System.nanoTime();
    return new Result(peakMemory.get(), endTime - startTime);
  }

  /**
   * Tracks peak memory usage and duration while running the given operation that returns a value.
   *
   * @param operation the operation to run and measure
   * @param <T> the return type of the operation
   * @return a pair containing the operation result and the measurement
   */
  public static <T> TrackedResult<T> trackWithResult(Supplier<T> operation) {
    Runtime runtime = Runtime.getRuntime();
    AtomicLong peakMemory = new AtomicLong(0);
    AtomicBoolean running = new AtomicBoolean(true);

    Thread monitor =
        new Thread(
            () -> {
              while (running.get()) {
                long used = runtime.totalMemory() - runtime.freeMemory();
                peakMemory.updateAndGet(current -> Math.max(current, used));
                try {
                  Thread.sleep(POLL_INTERVAL_MS);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  break;
                }
              }
            });
    monitor.setDaemon(true);

    long startTime = System.nanoTime();
    monitor.start();

    T result;
    try {
      result = operation.get();
    } finally {
      running.set(false);
      try {
        monitor.join(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    long endTime = System.nanoTime();
    return new TrackedResult<>(result, new Result(peakMemory.get(), endTime - startTime));
  }

  public static class TrackedResult<T> {
    private final T value;
    private final Result metrics;

    TrackedResult(T value, Result metrics) {
      this.value = value;
      this.metrics = metrics;
    }

    public T value() {
      return value;
    }

    public Result metrics() {
      return metrics;
    }
  }
}
