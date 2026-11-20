package com.dataorchestrator.alert;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

/**
 * Default alert delivery: writes a structured log record per alert and de-duplicates
 * repeated alerts for the same (job, type) within a short window so bursts do not spam the
 * operations team. Replace with a PagerDuty/Slack/email implementation in production.
 */
@Component
public class LoggingAlertService implements AlertService {

    private static final Logger log = LoggerFactory.getLogger(LoggingAlertService.class);
    private static final Duration DEDUP_WINDOW = Duration.ofMinutes(5);

    private final Map<String, Instant> recentAlerts = new ConcurrentHashMap<>();

    @Override
    public void alert(Alert alert) {
        String key = alert.jobId() + "|" + alert.alertType();
        Instant now = Instant.now();
        Instant last = recentAlerts.putIfAbsent(key, now);
        if (last != null && Duration.between(last, now).compareTo(DEDUP_WINDOW) < 0) {
            return; // duplicate suppressed
        }
        recentAlerts.put(key, now);

        // Best-effort cleanup of stale dedup entries.
        recentAlerts.entrySet().removeIf(e -> Duration.between(e.getValue(), now).compareTo(DEDUP_WINDOW) > 0);

        MDC.put("correlationId", alert.correlationId() == null ? "-" : alert.correlationId());
        try {
            switch (alert.severity()) {
                case CRITICAL -> log.error(
                    "ALERT severity=CRITICAL type={} jobId={} jobType={} executionId={} attempt={} reason={}",
                    alert.alertType(), alert.jobId(), alert.jobType(),
                    alert.executionId(), alert.attemptNumber(), alert.reason());
                case WARNING -> log.warn(
                    "ALERT severity=WARNING type={} jobId={} jobType={} executionId={} attempt={} reason={}",
                    alert.alertType(), alert.jobId(), alert.jobType(),
                    alert.executionId(), alert.attemptNumber(), alert.reason());
                default -> log.info(
                    "ALERT severity=INFO type={} jobId={} jobType={} executionId={} attempt={} reason={}",
                    alert.alertType(), alert.jobId(), alert.jobType(),
                    alert.executionId(), alert.attemptNumber(), alert.reason());
            }
        } finally {
            MDC.remove("correlationId");
        }
    }
}
