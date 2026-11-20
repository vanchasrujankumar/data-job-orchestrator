package com.dataorchestrator.job;

import java.util.Set;

/**
 * Logical lifecycle state of an orchestrated job.
 *
 * <p>The logical status is maintained independently from physical Dataproc execution
 * attempts. Transitions are validated by {@link JobStateMachine} so that invalid moves
 * (e.g. a succeeded job being re-queued) are rejected.</p>
 */
public enum JobStatus {

    /** Request consumed from Kafka and persisted; not yet eligible for dispatch. */
    RECEIVED,

    /** Eligible for dispatch, waiting for an available concurrency slot. */
    QUEUED,

    /** A concurrency slot has been reserved and the job is being submitted to Dataproc. */
    DISPATCHING,

    /** Dataproc accepted the submission; awaiting the Dataproc job to start. */
    SUBMITTED,

    /** The Dataproc job is actively running. */
    RUNNING,

    /** A transient failure occurred; waiting out an exponential backoff before retry. */
    RETRY_WAIT,

    /** The Dataproc job completed successfully. */
    SUCCEEDED,

    /** The job failed permanently (non-retryable or retries exhausted). */
    FAILED,

    /** Cancellation has been requested and is awaiting confirmation from Dataproc. */
    CANCEL_REQUESTED,

    /** The job was cancelled and cancellation confirmed. */
    CANCELLED,

    /** The job exceeded its configured execution timeout. */
    TIMED_OUT;

    /** Statuses that hold a reserved concurrency slot. */
    private static final Set<JobStatus> SLOT_HOLDING = Set.of(DISPATCHING, SUBMITTED, RUNNING, CANCEL_REQUESTED);

    /** Terminal states: no further automatic transitions. */
    private static final Set<JobStatus> TERMINAL = Set.of(SUCCEEDED, FAILED, CANCELLED, TIMED_OUT);

    public boolean isSlotHolding() {
        return SLOT_HOLDING.contains(this);
    }

    public boolean isTerminal() {
        return TERMINAL.contains(this);
    }
}
