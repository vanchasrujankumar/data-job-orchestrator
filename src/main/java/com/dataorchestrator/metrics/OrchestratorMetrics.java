package com.dataorchestrator.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.stereotype.Component;

import com.dataorchestrator.config.OrchestratorProperties;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;

/**
 * OpenTelemetry metrics for job throughput, queue/active levels, durations, retries and
 * Dataproc API failures. Only low-cardinality labels (job type) are used; the job id is
 * deliberately never a label to avoid unbounded metric cardinality.
 */
@Component
public class OrchestratorMetrics {

    private static final AttributeKey<String> JOB_TYPE = AttributeKey.stringKey("job.type");
    private static final AttributeKey<String> SCOPE = AttributeKey.stringKey("scope");

    private final LongCounter jobsReceived;
    private final LongUpDownCounter jobsQueued;
    private final LongUpDownCounter jobsActive;
    private final LongCounter jobsSucceeded;
    private final LongCounter jobsFailed;
    private final LongCounter jobsRetried;
    private final DoubleHistogram executionDuration;
    private final DoubleHistogram queueWait;
    private final LongCounter dataprocApiFailures;

    /** Reserved slots per job type, maintained for the capacity-utilization gauge. */
    private final Map<String, AtomicInteger> reservedByType = new ConcurrentHashMap<>();
    private final AtomicInteger globalReserved = new AtomicInteger();

    public OrchestratorMetrics(Meter meter, OrchestratorProperties props) {
        this.jobsReceived = meter.counterBuilder("orchestrator.jobs.received")
            .setDescription("Total job requests received from Kafka").build();
        this.jobsQueued = meter.upDownCounterBuilder("orchestrator.jobs.queued")
            .setDescription("Jobs currently waiting for a concurrency slot").build();
        this.jobsActive = meter.upDownCounterBuilder("orchestrator.jobs.active")
            .setDescription("Jobs currently holding a concurrency slot").build();
        this.jobsSucceeded = meter.counterBuilder("orchestrator.jobs.succeeded")
            .setDescription("Jobs that completed successfully").build();
        this.jobsFailed = meter.counterBuilder("orchestrator.jobs.failed")
            .setDescription("Jobs that failed permanently").build();
        this.jobsRetried = meter.counterBuilder("orchestrator.jobs.retried")
            .setDescription("Retry attempts triggered").build();
        this.executionDuration = meter.histogramBuilder("orchestrator.jobs.execution.duration")
            .setDescription("Execution duration in seconds").setUnit("s").build();
        this.queueWait = meter.histogramBuilder("orchestrator.jobs.queue.wait")
            .setDescription("Queue wait time in seconds").setUnit("s").build();
        this.dataprocApiFailures = meter.counterBuilder("orchestrator.dataproc.api.failures")
            .setDescription("Dataproc API call failures").build();

        // Seed per-type slot counters so utilization reports zero even when idle.
        props.concurrency().jobTypes().keySet().forEach(t -> reservedByType.putIfAbsent(t, new AtomicInteger()));

        meter.gaugeBuilder("orchestrator.scheduler.capacity.utilization")
            .setDescription("Concurrency utilization percentage (0-100) per job type")
            .setUnit("%")
            .ofLongs()
            .buildWithCallback(obs -> {
                reservedByType.forEach((type, count) ->
                    obs.record(utilization(count.get(), props.concurrencyLimit(type)), attrs(type)));
                obs.record(utilization(globalReserved.get(), props.concurrency().globalMaxJobs()),
                    Attributes.of(SCOPE, "global"));
            });
    }

    public void jobReceived(String jobType) {
        jobsReceived.add(1, attrs(jobType));
    }

    public void jobQueued(String jobType) {
        jobsQueued.add(1, attrs(jobType));
    }

    public void jobDequeued(String jobType) {
        jobsQueued.add(-1, attrs(jobType));
    }

    public void slotAcquired(String jobType) {
        jobsActive.add(1, attrs(jobType));
        reservedByType.computeIfAbsent(jobType, k -> new AtomicInteger()).incrementAndGet();
        globalReserved.incrementAndGet();
    }

    public void slotReleased(String jobType) {
        jobsActive.add(-1, attrs(jobType));
        reservedByType.computeIfAbsent(jobType, k -> new AtomicInteger()).decrementAndGet();
        globalReserved.decrementAndGet();
    }

    public void jobSucceeded(String jobType) {
        jobsSucceeded.add(1, attrs(jobType));
    }

    public void jobFailed(String jobType) {
        jobsFailed.add(1, attrs(jobType));
    }

    public void jobRetried(String jobType) {
        jobsRetried.add(1, attrs(jobType));
    }

    public void executionDuration(String jobType, double seconds) {
        executionDuration.record(seconds, attrs(jobType));
    }

    public void queueWait(String jobType, double seconds) {
        queueWait.record(seconds, attrs(jobType));
    }

    public void dataprocApiFailure(String jobType) {
        dataprocApiFailures.add(1, attrs(jobType));
    }

    private long utilization(int reserved, int limit) {
        if (limit <= 0) {
            return 0;
        }
        return Math.min(100, (long) reserved * 100 / limit);
    }

    private Attributes attrs(String jobType) {
        return Attributes.of(JOB_TYPE, jobType == null ? "unknown" : jobType);
    }
}
