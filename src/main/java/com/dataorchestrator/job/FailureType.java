package com.dataorchestrator.job;

/**
 * Classification of a job execution failure, used to decide whether a retry is allowed.
 */
public enum FailureType {

    /** A transient error (network, timeout, retryable Dataproc state); eligible for retry. */
    TRANSIENT,

    /** A permanent error (invalid configuration, permissions); retries will not help. */
    PERMANENT,

    /** The execution exceeded its configured timeout. */
    TIMEOUT,

    /** The outcome of a submission could not be determined; needs reconciliation. */
    AMBIGUOUS
}
