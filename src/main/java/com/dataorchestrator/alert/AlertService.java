package com.dataorchestrator.alert;

/**
 * Port for delivering operational alerts. The organisation's alerting platform (PagerDuty,
 * Slack, email, etc.) is plugged in by implementing this interface; the default
 * {@link LoggingAlertService} writes structured log records and de-duplicates bursts.
 */
public interface AlertService {

    /**
     * Delivers an alert. Implementations must be idempotent or de-duplicating and must not
     * throw for transient delivery failures.
     */
    void alert(Alert alert);
}
