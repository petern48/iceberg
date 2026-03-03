package org.apache.iceberg.dev;

public class WriteMetrics {
    // Counts
    public int totalRowGroups;
    public int totalDataFiles;
    // Memory Usage
    public float maxMemoryUsage;
    // Durations
    public float puffinWriteDuration;
    public float manifestWriteDuration;
    public float datafileWriteDuration;
    public float totalWriteDuration;
    // Disk Storage
    public float puffinDiskSizeInBytes;
    public float puffinFooterSizeInBytes;
}
