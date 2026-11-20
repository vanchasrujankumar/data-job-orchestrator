package com.dataorchestrator.scheduler;

import java.util.Map;

/**
 * Snapshot of scheduler state and concurrency utilisation, exposed via the operational API.
 */
public record SchedulerStatus(
    boolean paused,
    int globalMaxJobs,
    int globalReserved,
    Map<String, TypeUtilization> jobTypes
) {

    public record TypeUtilization(int limit, int reserved, int queued) {
    }
}
