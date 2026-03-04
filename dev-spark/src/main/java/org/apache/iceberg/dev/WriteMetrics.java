package org.apache.iceberg.dev;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class WriteMetrics {
  // Counts
  public Integer totalRowGroups;
  public Integer totalDataFiles;
  // Puffin stats file (from ComputeTableStats result)
  public Long puffinDiskSizeInBytes;
  public Long puffinFooterSizeInBytes;
  // Memory / durations — not instrumented on write path
  public Float maxMemoryUsage;
  public Float puffinWriteDuration;
  public Float manifestWriteDuration;
  public Float datafileWriteDuration;
  public Float totalWriteDuration;
}
