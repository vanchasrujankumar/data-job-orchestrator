package com.dataorchestrator.dataproc;

import java.time.Instant;

/**
 * Snapshot of a Dataproc execution's current state.
 */
public record DataprocStatus(DataprocState state, Instant stateTime, String detail) {
}
