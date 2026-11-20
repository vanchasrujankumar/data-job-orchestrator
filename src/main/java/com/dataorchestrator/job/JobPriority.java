package com.dataorchestrator.job;

/**
 * Dispatch priority for queued jobs. Higher priorities are selected first, with FIFO
 * ordering preserved within the same priority. A starvation guard promotes jobs that have
 * waited too long (see {@code OrchestratorProperties.Scheduling.starvationThreshold}).
 */
public enum JobPriority {
    HIGH,
    NORMAL,
    LOW
}
