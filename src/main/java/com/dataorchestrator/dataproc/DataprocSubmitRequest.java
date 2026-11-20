package com.dataorchestrator.dataproc;

/**
 * Request to submit a Spark workload to Dataproc. The {@code jobId} and
 * {@code attemptNumber} combine into a deterministic Dataproc request id so that a retried
 * submission is idempotent.
 */
public record DataprocSubmitRequest(
    String jobId,
    int attemptNumber,
    SparkJobSpec spec,
    String projectId,
    String region,
    String clusterName
) {
    /** Deterministic request id used for idempotent Dataproc submission. */
    public String requestId() {
        return jobId + "-attempt-" + attemptNumber;
    }
}
