package com.dataorchestrator.dataproc;

/**
 * Result of a Dataproc submission: the physical execution identifier and its initial state.
 */
public record DataprocSubmitResult(String dataprocJobId, DataprocState state) {
}
