package com.dataorchestrator.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dataorchestrator.common.InvalidStateTransitionException;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the logical job state machine.
 */
class JobStateMachineTest {

    @Test
    void receivedCanBeQueued() {
        JobStateMachine.assertValid(JobStatus.RECEIVED, JobStatus.QUEUED);
        assertThat(JobStateMachine.canTransition(JobStatus.RECEIVED, JobStatus.QUEUED)).isTrue();
    }

    @Test
    void queuedCanBeDispatchedAndCancelled() {
        assertThat(JobStateMachine.canTransition(JobStatus.QUEUED, JobStatus.DISPATCHING)).isTrue();
        assertThat(JobStateMachine.canTransition(JobStatus.QUEUED, JobStatus.CANCELLED)).isTrue();
    }

    @Test
    void runningCanReachTerminalStates() {
        assertThat(JobStateMachine.canTransition(JobStatus.RUNNING, JobStatus.SUCCEEDED)).isTrue();
        assertThat(JobStateMachine.canTransition(JobStatus.RUNNING, JobStatus.FAILED)).isTrue();
        assertThat(JobStateMachine.canTransition(JobStatus.RUNNING, JobStatus.CANCELLED)).isTrue();
        assertThat(JobStateMachine.canTransition(JobStatus.RUNNING, JobStatus.TIMED_OUT)).isTrue();
    }

    @Test
    void terminalStatesHaveNoTransitions() {
        assertThat(JobStateMachine.canTransition(JobStatus.SUCCEEDED, JobStatus.QUEUED)).isFalse();
        assertThat(JobStateMachine.canTransition(JobStatus.CANCELLED, JobStatus.RUNNING)).isFalse();
    }

    @Test
    void failedCanBeManuallyRetried() {
        assertThat(JobStateMachine.canTransition(JobStatus.FAILED, JobStatus.QUEUED)).isTrue();
        assertThat(JobStateMachine.canTransition(JobStatus.TIMED_OUT, JobStatus.QUEUED)).isTrue();
    }

    @Test
    void invalidTransitionIsRejected() {
        assertThatThrownBy(() -> JobStateMachine.assertValid(JobStatus.QUEUED, JobStatus.SUCCEEDED))
            .isInstanceOf(InvalidStateTransitionException.class);
    }

    @Test
    void sameStateIsAllowed() {
        JobStateMachine.assertValid(JobStatus.RUNNING, JobStatus.RUNNING);
    }
}
