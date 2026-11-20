package com.dataorchestrator.dataproc;

/**
 * Normalised execution state returned by the Dataproc client. Shields the rest of the
 * service from the underlying Google Cloud SDK state vocabulary.
 */
public enum DataprocState {

    /** Submitted but not yet running. */
    PENDING,

    /** Actively executing. */
    RUNNING,

    /** Completed successfully. */
    SUCCEEDED,

    /** Completed with an error. */
    FAILED,

    /** Cancelled. */
    CANCELLED,

    /** State could not be determined. */
    UNKNOWN
}
