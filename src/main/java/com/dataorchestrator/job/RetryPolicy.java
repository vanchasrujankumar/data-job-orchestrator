package com.dataorchestrator.job;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import com.dataorchestrator.config.OrchestratorProperties;

/**
 * Computes exponential backoff delays with jitter for retry scheduling.
 */
public final class RetryPolicy {

    private RetryPolicy() {
    }

    /**
     * Computes the backoff delay after {@code attemptsSoFar} failures, bounded by the
     * configured maximum delay. "Equal jitter" is applied (uniform in the second half of the
     * capped window) so the result always stays within {@code [delay/2, delay]} and never
     * exceeds {@code max-delay}, while still avoiding a thundering herd.
     */
    public static Duration computeBackoff(int attemptsSoFar, OrchestratorProperties.Retry config) {
        double multiplier = Math.pow(config.multiplier(), Math.max(0, attemptsSoFar - 1));
        double baseMillis = config.initialDelay().toMillis() * multiplier;
        long cappedMillis = (long) Math.min(baseMillis, config.maxDelay().toMillis());
        long jittered = cappedMillis / 2 + ThreadLocalRandom.current().nextLong(cappedMillis / 2 + 1);
        return Duration.ofMillis(jittered);
    }
}
