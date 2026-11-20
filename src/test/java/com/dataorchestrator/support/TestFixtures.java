package com.dataorchestrator.support;

import java.time.Duration;
import java.util.Map;

import com.dataorchestrator.config.OrchestratorProperties;

/**
 * Shared test fixtures for building configuration and domain objects.
 */
public final class TestFixtures {

    private TestFixtures() {
    }

    /** Returns a fully-populated {@link OrchestratorProperties} with test-friendly defaults. */
    public static OrchestratorProperties props() {
        return new OrchestratorProperties(
            new OrchestratorProperties.Kafka("job-requests", "job-events", "job-requests-dlt",
                "test-group", 2),
            new OrchestratorProperties.Concurrency(10, Map.of("SEGMENT_PREVIEW", 5),
                100, Duration.ofSeconds(60)),
            new OrchestratorProperties.Retry(3, Duration.ofSeconds(60), 2.0, Duration.ofSeconds(900)),
            new OrchestratorProperties.Dataproc("noop", "project", "us-central1", "cluster", null),
            new OrchestratorProperties.Timeout(Duration.ofHours(1), Map.of()),
            new OrchestratorProperties.Scheduling(Duration.ofSeconds(5), Duration.ofSeconds(15),
                Duration.ofSeconds(15), Duration.ofSeconds(60), Duration.ofSeconds(5),
                Duration.ofMinutes(5)),
            new OrchestratorProperties.Otel(false, "data-job-orchestrator", null),
            new OrchestratorProperties.Alerting(Duration.ofMinutes(30))
        );
    }
}
