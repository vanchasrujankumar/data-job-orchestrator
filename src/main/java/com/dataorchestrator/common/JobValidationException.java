package com.dataorchestrator.common;

/**
 * Thrown when an incoming Kafka job request payload fails validation. Marked as
 * non-retryable so the Kafka error handler routes it to the dead-letter topic.
 */
public class JobValidationException extends RuntimeException {
    public JobValidationException(String message) {
        super(message);
    }
}
