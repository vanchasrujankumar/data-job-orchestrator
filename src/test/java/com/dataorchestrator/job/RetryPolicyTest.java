package com.dataorchestrator.job;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import com.dataorchestrator.config.OrchestratorProperties;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for retry backoff computation.
 */
class RetryPolicyTest {

    private final OrchestratorProperties.Retry config =
        new OrchestratorProperties.Retry(3, Duration.ofSeconds(60), 2.0, Duration.ofSeconds(900));

    @Test
    void firstBackoffIsWithinInitialDelayWindow() {
        Duration delay = RetryPolicy.computeBackoff(1, config);
        // Equal jitter keeps the first delay in [initial/2, initial].
        assertThat(delay).isBetween(Duration.ofSeconds(30), Duration.ofSeconds(60));
    }

    @Test
    void backoffIsCappedAtMaxDelay() {
        for (int i = 1; i <= 10; i++) {
            Duration delay = RetryPolicy.computeBackoff(i, config);
            assertThat(delay).isLessThanOrEqualTo(Duration.ofSeconds(900));
        }
    }

    @Test
    void backoffGrowsWithAttempts() {
        Duration first = RetryPolicy.computeBackoff(1, config);
        Duration third = RetryPolicy.computeBackoff(3, config);
        assertThat(third).isGreaterThan(first);
    }
}
