package com.dataorchestrator.scheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.dataorchestrator.config.InstanceIdentity;
import com.dataorchestrator.config.OrchestratorProperties;
import com.dataorchestrator.dataproc.DataprocClient;
import com.dataorchestrator.dataproc.DataprocException;
import com.dataorchestrator.dataproc.DataprocSubmitRequest;
import com.dataorchestrator.dataproc.DataprocSubmitResult;
import com.dataorchestrator.dataproc.SparkJobSpec;
import com.dataorchestrator.job.FailureClassifier;
import com.dataorchestrator.job.FailureType;
import com.dataorchestrator.job.JobEntity;
import com.dataorchestrator.job.JobPriority;
import com.dataorchestrator.job.JobRepository;
import com.dataorchestrator.job.JobService;
import com.dataorchestrator.job.JobStatus;
import com.dataorchestrator.job.RetryPolicy;
import com.dataorchestrator.metrics.OrchestratorMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Periodically claims eligible queued jobs and submits them to Dataproc asynchronously,
 * honouring global and per-job-type concurrency limits. Claims are atomic so a single job
 * is never dispatched by two replicas simultaneously.
 */
@Component
public class JobDispatcher {

    private static final Logger log = LoggerFactory.getLogger(JobDispatcher.class);

    private final JobRepository jobRepository;
    private final JobService jobService;
    private final ConcurrencyService concurrencyService;
    private final SchedulerControlService schedulerControl;
    private final DataprocClient dataprocClient;
    private final OrchestratorProperties props;
    private final InstanceIdentity instanceIdentity;
    private final OrchestratorMetrics metrics;
    private final ObjectMapper objectMapper;

    public JobDispatcher(JobRepository jobRepository, JobService jobService,
                         ConcurrencyService concurrencyService,
                         SchedulerControlService schedulerControl, DataprocClient dataprocClient,
                         OrchestratorProperties props, InstanceIdentity instanceIdentity,
                         OrchestratorMetrics metrics, ObjectMapper objectMapper) {
        this.jobRepository = jobRepository;
        this.jobService = jobService;
        this.concurrencyService = concurrencyService;
        this.schedulerControl = schedulerControl;
        this.dataprocClient = dataprocClient;
        this.props = props;
        this.instanceIdentity = instanceIdentity;
        this.metrics = metrics;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedDelayString = "${orchestrator.scheduling.dispatch-interval}")
    public void dispatch() {
        if (schedulerControl.isPaused()) {
            return;
        }
        int batch = Math.max(1, props.concurrency().dispatchBatchSize());
        List<JobEntity> queued = jobRepository.findByStatus(JobStatus.QUEUED,
            PageRequest.of(0, batch)).getContent();
        List<JobEntity> ordered = queued.stream()
            .sorted(priorityComparator())
            .toList();

        for (JobEntity candidate : ordered) {
            if (!concurrencyService.hasCapacity(candidate.getJobType())) {
                continue;
            }
            Instant leaseExpiry = Instant.now().plus(props.concurrency().leaseDuration());
            Optional<JobEntity> claimed = jobService.claim(candidate.getId(),
                instanceIdentity.id(), leaseExpiry);
            claimed.ifPresent(this::dispatchOne);
        }
    }

    private void dispatchOne(JobEntity job) {
        SparkJobSpec spec = parseSpec(job);
        String project = props.dataproc().projectId();
        String region = props.dataproc().region();
        String cluster = spec.clusterName() != null ? spec.clusterName() : props.dataproc().clusterName();

        int attemptNumber = jobService.beginAttempt(job.getJobId());
        DataprocSubmitRequest request = new DataprocSubmitRequest(
            job.getJobId(), attemptNumber, spec, project, region, cluster);

        try {
            DataprocSubmitResult result = dataprocClient.submit(request);
            jobService.markSubmitted(job.getJobId(), result.dataprocJobId(), project, region, cluster);
        } catch (DataprocException e) {
            handleSubmissionFailure(job, e);
        } catch (Exception e) {
            handleSubmissionFailure(job, new DataprocException("Unexpected submission error", e));
        }
    }

    private void handleSubmissionFailure(JobEntity job, DataprocException e) {
        metrics.dataprocApiFailure(job.getJobType());
        FailureType type = FailureClassifier.classify(e);
        int failedAttempt = job.getAttemptCount();
        log.warn("Dataproc submission failed jobId={} attempt={} type={} reason={}",
            job.getJobId(), failedAttempt, type, e.getMessage());

        if (type == FailureType.PERMANENT || failedAttempt >= job.getMaxAttempts()) {
            jobService.failPermanent(job.getJobId(), type, e.getMessage());
            return;
        }
        Instant nextRetry = Instant.now().plus(RetryPolicy.computeBackoff(failedAttempt, props.retry()));
        jobService.failRetryable(job.getJobId(), type, e.getMessage(), nextRetry);
    }

    private SparkJobSpec parseSpec(JobEntity job) {
        try {
            return objectMapper.readValue(job.getExecutionConfig(), SparkJobSpec.class);
        } catch (Exception e) {
            throw new DataprocException("Failed to parse execution config for job "
                + job.getJobId(), e);
        }
    }

    private Comparator<JobEntity> priorityComparator() {
        Duration threshold = props.scheduling().starvationThreshold();
        return Comparator
            .comparingInt((JobEntity j) -> effectivePriority(j, threshold))
            .thenComparing(JobEntity::getCreatedAt);
    }

    private int effectivePriority(JobEntity job, Duration threshold) {
        int rank = priorityRank(job.getPriority());
        if (rank > 0 && threshold != null && !threshold.isZero() && !threshold.isNegative()) {
            long ageMillis = Duration.between(job.getCreatedAt(), Instant.now()).toMillis();
            int promotions = (int) (ageMillis / threshold.toMillis());
            rank = Math.max(0, rank - promotions);
        }
        return rank;
    }

    private int priorityRank(JobPriority priority) {
        return switch (priority) {
            case HIGH -> 0;
            case NORMAL -> 1;
            case LOW -> 2;
        };
    }
}
