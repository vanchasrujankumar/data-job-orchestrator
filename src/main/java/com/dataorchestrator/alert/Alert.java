package com.dataorchestrator.alert;

/**
 * Immutable alert describing a critical job condition (permanent failure, timeout, stuck
 * job or excessive queue delay). Contains enough context to trace and troubleshoot.
 */
public record Alert(
    AlertSeverity severity,
    String alertType,
    String jobId,
    String jobType,
    String correlationId,
    String executionId,
    String reason,
    int attemptNumber
) {
}
