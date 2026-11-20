package com.dataorchestrator.job;

import java.util.Map;
import java.util.Set;

import com.dataorchestrator.common.InvalidStateTransitionException;

/**
 * Validates that a job only moves between {@link JobStatus} states that make sense.
 *
 * <p>This is the single source of truth for the logical state machine. Any write path
 * that changes a job's status must go through {@link #assertValid(JobStatus, JobStatus)}
 * (or a higher-level service method that does so).</p>
 */
public final class JobStateMachine {

    private static final Map<JobStatus, Set<JobStatus>> ALLOWED = Map.ofEntries(
        Map.entry(JobStatus.RECEIVED, Set.of(JobStatus.QUEUED, JobStatus.FAILED, JobStatus.CANCELLED)),
        Map.entry(JobStatus.QUEUED, Set.of(JobStatus.DISPATCHING, JobStatus.CANCELLED)),
        Map.entry(JobStatus.DISPATCHING,
            Set.of(JobStatus.SUBMITTED, JobStatus.RETRY_WAIT, JobStatus.FAILED,
                JobStatus.CANCEL_REQUESTED, JobStatus.QUEUED)),
        Map.entry(JobStatus.SUBMITTED,
            Set.of(JobStatus.RUNNING, JobStatus.SUCCEEDED, JobStatus.FAILED,
                JobStatus.RETRY_WAIT, JobStatus.TIMED_OUT, JobStatus.CANCEL_REQUESTED, JobStatus.CANCELLED)),
        Map.entry(JobStatus.RUNNING,
            Set.of(JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.RETRY_WAIT,
                JobStatus.TIMED_OUT, JobStatus.CANCEL_REQUESTED, JobStatus.CANCELLED)),
        Map.entry(JobStatus.RETRY_WAIT,
            Set.of(JobStatus.QUEUED, JobStatus.FAILED, JobStatus.CANCELLED)),
        Map.entry(JobStatus.CANCEL_REQUESTED,
            Set.of(JobStatus.CANCELLED, JobStatus.FAILED, JobStatus.RETRY_WAIT, JobStatus.TIMED_OUT)),
        // Manual retry of a terminal failure re-queues the job (creates a new attempt).
        Map.entry(JobStatus.FAILED, Set.of(JobStatus.QUEUED)),
        Map.entry(JobStatus.TIMED_OUT, Set.of(JobStatus.QUEUED)),
        Map.entry(JobStatus.SUCCEEDED, Set.of()),
        Map.entry(JobStatus.CANCELLED, Set.of())
    );

    private JobStateMachine() {
    }

    public static void assertValid(JobStatus from, JobStatus to) {
        Set<JobStatus> allowed = ALLOWED.getOrDefault(from, Set.of());
        if (to != from && !allowed.contains(to)) {
            throw new InvalidStateTransitionException(
                "Invalid job state transition: " + from + " -> " + to);
        }
    }

    public static boolean canTransition(JobStatus from, JobStatus to) {
        return to == from || ALLOWED.getOrDefault(from, Set.of()).contains(to);
    }
}
