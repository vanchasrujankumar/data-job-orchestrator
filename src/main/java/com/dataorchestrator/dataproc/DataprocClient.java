package com.dataorchestrator.dataproc;

/**
 * Port for Dataproc job submission, status polling and cancellation. Implementations are
 * swappable via {@code orchestrator.dataproc.provider}: {@code gcp} (real SDK) or
 * {@code noop} (local/dev). Tests inject an in-memory fake.
 */
public interface DataprocClient {

    /**
     * Submits a Spark job asynchronously and returns the physical execution identifier.
     */
    DataprocSubmitResult submit(DataprocSubmitRequest request);

    /**
     * Polls the current state of a previously submitted execution.
     */
    DataprocStatus getStatus(String dataprocJobId, String projectId, String region);

    /**
     * Requests cancellation of a running execution.
     */
    void cancel(String dataprocJobId, String projectId, String region);
}
