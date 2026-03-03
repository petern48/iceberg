package org.apache.iceberg.dev;

public class ReadMetrics {
    // Counts
    public int skippedRowGroups;  // note get totalRowGroups from WriteMetrics
    public int skippedDataFiles;  // note get totalDataFiles from WriteMetrics
    // Memory Usage
    public float maxMemoryUsage;
    // Durations
    public float puffinReadDuration;
    public float manifestReadDuration;
    public float datafileReadDuration;
    public float totalReadDuration;
}
