package com.dataorchestrator.config;

import java.time.Duration;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Typed configuration for the orchestrator, bound from the {@code orchestrator.*}
 * namespace. All timing and limit values are environment-driven so behaviour can change
 * without redeploying code.
 */
@ConfigurationProperties(prefix = "orchestrator")
public record OrchestratorProperties(
    Kafka kafka,
    Concurrency concurrency,
    Retry retry,
    Dataproc dataproc,
    Timeout timeout,
    Scheduling scheduling,
    Otel otel,
    Alerting alerting
) {

    public record Kafka(String jobRequestTopic, String jobEventsTopic, String deadLetterTopic,
                        String consumerGroup, int consumerConcurrency) {
    }

    public record Concurrency(int globalMaxJobs, Map<String, Integer> jobTypes,
                              int dispatchBatchSize, Duration leaseDuration) {
    }

    public record Retry(int maxAttempts, Duration initialDelay, double multiplier,
                        Duration maxDelay) {
    }

    public record Dataproc(String provider, String projectId, String region,
                           String clusterName, String endpoint) {
    }

    public record Timeout(Duration defaultMaxDuration, Map<String, Duration> byJobType) {
    }

    public record Scheduling(Duration dispatchInterval, Duration monitorInterval,
                             Duration retryInterval, Duration reconcileInterval,
                             Duration outboxInterval, Duration starvationThreshold) {
    }

    public record Otel(boolean enabled, String serviceName, String otlpEndpoint) {
    }

    public record Alerting(Duration queueDelayThreshold) {
    }

    /** Resolves the per-job-type execution timeout, falling back to the default. */
    public Duration executionTimeout(String jobType) {
        return timeout.byJobType().getOrDefault(jobType, timeout.defaultMaxDuration());
    }

    /** Resolves the per-job-type concurrency limit, falling back to the global limit. */
    public int concurrencyLimit(String jobType) {
        return concurrency.jobTypes().getOrDefault(jobType, concurrency.globalMaxJobs());
    }
}
