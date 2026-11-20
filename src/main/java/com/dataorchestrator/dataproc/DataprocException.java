package com.dataorchestrator.dataproc;

/**
 * Wraps any failure talking to the Dataproc API (network, auth, rejected submission, etc.).
 * Callers can classify these as retryable or permanent.
 */
public class DataprocException extends RuntimeException {
    public DataprocException(String message) {
        super(message);
    }

    public DataprocException(String message, Throwable cause) {
        super(message, cause);
    }
}
