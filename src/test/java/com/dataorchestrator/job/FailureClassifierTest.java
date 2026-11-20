package com.dataorchestrator.job;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.dataorchestrator.dataproc.DataprocException;

/**
 * Unit tests for failure classification.
 */
class FailureClassifierTest {

    // Simulates com.google.api.gax.rpc.InvalidArgumentException by name.
    private static class InvalidArgumentException extends RuntimeException {
    }

    // Simulates com.google.api.gax.rpc.PermissionDeniedException by name.
    private static class PermissionDeniedException extends RuntimeException {
    }

    @Test
    void invalidArgumentIsPermanent() {
        Exception cause = new InvalidArgumentException();
        assertThat(FailureClassifier.classify(new DataprocException("x", cause)))
            .isEqualTo(FailureType.PERMANENT);
    }

    @Test
    void permissionDeniedIsPermanent() {
        Exception cause = new PermissionDeniedException();
        assertThat(FailureClassifier.classify(new DataprocException("x", cause)))
            .isEqualTo(FailureType.PERMANENT);
    }

    @Test
    void transientErrorIsRetryable() {
        Exception cause = new java.net.ConnectException("connection refused");
        assertThat(FailureClassifier.classify(new DataprocException("x", cause)))
            .isEqualTo(FailureType.TRANSIENT);
    }

    @Test
    void unknownErrorDefaultsToTransient() {
        assertThat(FailureClassifier.classify(new RuntimeException("boom")))
            .isEqualTo(FailureType.TRANSIENT);
    }
}
