package com.dataorchestrator.common;

/**
 * Thrown when an operation targets a job that does not exist.
 */
public class JobNotFoundException extends RuntimeException {
    public JobNotFoundException(String jobId) {
        super("Job not found: " + jobId);
    }
}
