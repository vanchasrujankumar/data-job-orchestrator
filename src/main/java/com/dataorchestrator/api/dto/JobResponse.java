package com.dataorchestrator.api.dto;

import java.time.Instant;

import com.dataorchestrator.job.JobEntity;
import com.dataorchestrator.job.JobPriority;
import com.dataorchestrator.job.JobStatus;

/**
 * Read model for a logical job, safe to serialise (no persistence internals).
 */
public record JobResponse(
    String jobId,
    String jobType,
    JobPriority priority,
    JobStatus status,
    int attemptCount,
    int maxAttempts,
    String correlationId,
    String dataprocJobId,
    String lastError,
    Instant receivedAt,
    Instant queuedAt,
    Instant submittedAt,
    Instant startedAt,
    Instant completedAt,
    Instant nextRetryAt
) {
    /** Maps an entity to its read model. */
    public static JobResponse from(JobEntity job) {
        return new JobResponse(
            job.getJobId(),
            job.getJobType(),
            job.getPriority(),
            job.getStatus(),
            job.getAttemptCount(),
            job.getMaxAttempts(),
            job.getCorrelationId(),
            job.getDataprocJobId(),
            job.getLastError(),
            job.getReceivedAt(),
            job.getQueuedAt(),
            job.getSubmittedAt(),
            job.getStartedAt(),
            job.getCompletedAt(),
            job.getNextRetryAt()
        );
    }
}
