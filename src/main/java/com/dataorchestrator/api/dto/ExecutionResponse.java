package com.dataorchestrator.api.dto;

import java.time.Instant;

import com.dataorchestrator.job.ExecutionAttemptEntity;
import com.dataorchestrator.job.FailureType;
import com.dataorchestrator.job.JobStatus;

/**
 * Read model for a single execution attempt.
 */
public record ExecutionResponse(
    int attemptNumber,
    JobStatus status,
    String dataprocJobId,
    FailureType errorType,
    String errorMessage,
    Instant submittedAt,
    Instant startedAt,
    Instant completedAt
) {
    /** Maps an attempt entity to its read model. */
    public static ExecutionResponse from(ExecutionAttemptEntity attempt) {
        return new ExecutionResponse(
            attempt.getAttemptNumber(),
            attempt.getStatus(),
            attempt.getDataprocJobId(),
            attempt.getErrorType(),
            attempt.getErrorMessage(),
            attempt.getSubmittedAt(),
            attempt.getStartedAt(),
            attempt.getCompletedAt()
        );
    }
}
