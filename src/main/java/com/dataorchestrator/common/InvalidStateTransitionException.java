package com.dataorchestrator.common;

/**
 * Thrown when a job is asked to transition between two logical states that are not allowed
 * by the {@code JobStateMachine}.
 */
public class InvalidStateTransitionException extends RuntimeException {
    public InvalidStateTransitionException(String message) {
        super(message);
    }
}
