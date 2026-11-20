package com.dataorchestrator.common;

/**
 * Thrown when a manual retry or cancellation is requested for a job that is not in an
 * eligible state.
 */
public class JobConflictException extends RuntimeException {
    public JobConflictException(String message) {
        super(message);
    }
}
