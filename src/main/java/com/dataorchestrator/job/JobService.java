package com.dataorchestrator.job;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.dataorchestrator.alert.Alert;
import com.dataorchestrator.alert.AlertService;
import com.dataorchestrator.alert.AlertSeverity;
import com.dataorchestrator.common.JobConflictException;
import com.dataorchestrator.common.JobNotFoundException;
import com.dataorchestrator.config.OrchestratorProperties;
import com.dataorchestrator.metrics.OrchestratorMetrics;
import com.dataorchestrator.outbox.OutboxService;

/**
 * Core write-path service. Owns the logical state machine, execution-attempt bookkeeping,
 * retry scheduling, alerting and outbox event publication. Every state change flows through
 * {@link #transition} so transitions are validated and the audit trail is always written.
 */
@Service
public class JobService {

    private final JobRepository jobRepository;
    private final ExecutionAttemptRepository attemptRepository;
    private final JobStatusHistoryRepository historyRepository;
    private final MongoTemplate mongoTemplate;
    private final OrchestratorMetrics metrics;
    private final OutboxService outboxService;
    private final AlertService alertService;
    private final OrchestratorProperties props;

    public JobService(JobRepository jobRepository, ExecutionAttemptRepository attemptRepository,
                      JobStatusHistoryRepository historyRepository, MongoTemplate mongoTemplate,
                      OrchestratorMetrics metrics, OutboxService outboxService,
                      AlertService alertService, OrchestratorProperties props) {
        this.jobRepository = jobRepository;
        this.attemptRepository = attemptRepository;
        this.historyRepository = historyRepository;
        this.mongoTemplate = mongoTemplate;
        this.metrics = metrics;
        this.outboxService = outboxService;
        this.alertService = alertService;
        this.props = props;
    }

    /** Persists a newly accepted job in the RECEIVED state. */
    @Transactional
    public JobEntity createJob(String eventId, String jobId, String correlationId, String jobType,
                               JobPriority priority, String executionConfig) {
        JobEntity job = JobEntity.builder()
            .jobId(jobId)
            .eventId(eventId)
            .correlationId(correlationId)
            .jobType(jobType)
            .priority(priority)
            .status(JobStatus.RECEIVED)
            .attemptCount(0)
            .maxAttempts(props.retry().maxAttempts())
            .executionConfig(executionConfig)
            .executionTimeoutSeconds((int) props.executionTimeout(jobType).toSeconds())
            .receivedAt(Instant.now())
            .build();
        job = jobRepository.save(job);
        recordHistory(job, null, JobStatus.RECEIVED, "Request accepted");
        metrics.jobReceived(jobType);
        return job;
    }

    /** Moves a RECEIVED job into the QUEUED state. */
    @Transactional
    public JobEntity queue(String jobId) {
        JobEntity job = get(jobId);
        transition(job, JobStatus.QUEUED, "Queued for dispatch");
        return job;
    }

    /**
     * Accepts a new job request: creates the RECEIVED record and queues it atomically in a
     * single transaction, so a job is never left stranded in RECEIVED.
     */
    @Transactional
    public JobEntity accept(String eventId, String jobId, String correlationId, String jobType,
                            JobPriority priority, String executionConfig) {
        JobEntity job = createJob(eventId, jobId, correlationId, jobType, priority, executionConfig);
        return queue(job.getJobId());
    }

    /**
     * Atomically claims a queued job for dispatch using a MongoDB {@code findAndModify}.
     * Returns an empty result if another replica claimed it first. The returned document is
     * already in DISPATCHING.
     */
    public Optional<JobEntity> claim(String id, String owner, Instant leaseExpiry) {
        Instant now = Instant.now();
        Query query = new Query(Criteria.where("_id").is(id).and("status").is(JobStatus.QUEUED));
        Update update = new Update()
            .set("status", JobStatus.DISPATCHING)
            .set("leaseOwner", owner)
            .set("leaseExpiresAt", leaseExpiry)
            .set("dispatchedAt", now)
            .set("updatedAt", now);
        JobEntity claimed = mongoTemplate.findAndModify(query, update,
            FindAndModifyOptions.options().returnNew(true), JobEntity.class);
        if (claimed == null) {
            return Optional.empty();
        }
        recordHistory(claimed, JobStatus.QUEUED, JobStatus.DISPATCHING, "Slot reserved");
        metrics.slotAcquired(claimed.getJobType());
        metrics.jobDequeued(claimed.getJobType());
        return Optional.of(claimed);
    }

    /** Records a successful Dataproc submission and updates the current execution attempt. */
    @Transactional
    public JobEntity markSubmitted(String jobId, String dataprocJobId, String projectId,
                                   String region, String clusterName) {
        JobEntity job = get(jobId);
        job.setDataprocJobId(dataprocJobId);
        job.setDataprocProjectId(projectId);
        job.setDataprocRegion(region);
        job.setDataprocClusterName(clusterName);
        transition(job, JobStatus.SUBMITTED, "Submitted to Dataproc");
        attemptRepository.findByJobIdAndAttemptNumber(jobId, job.getAttemptCount())
            .ifPresent(a -> {
                a.setStatus(JobStatus.SUBMITTED);
                a.setDataprocJobId(dataprocJobId);
                attemptRepository.save(a);
            });
        return job;
    }

    /**
     * Starts a new execution attempt before submission, so every attempt (including one that
     * fails at the Dataproc API boundary) is recorded independently. Returns the 1-based
     * attempt number.
     */
    @Transactional
    public int beginAttempt(String jobId) {
        JobEntity job = get(jobId);
        int attemptNumber = job.getAttemptCount() + 1;
        job.setAttemptCount(attemptNumber);
        jobRepository.save(job);
        attemptRepository.save(ExecutionAttemptEntity.builder()
            .jobId(jobId)
            .attemptNumber(attemptNumber)
            .status(JobStatus.DISPATCHING)
            .submittedAt(Instant.now())
            .build());
        return attemptNumber;
    }

    /** Marks a submitted job as running (idempotent). */
    @Transactional
    public JobEntity markRunning(String jobId) {
        JobEntity job = get(jobId);
        if (job.getStatus() == JobStatus.RUNNING) {
            return job;
        }
        transition(job, JobStatus.RUNNING, "Dataproc execution started");
        attemptRepository.findByJobIdAndAttemptNumber(jobId, job.getAttemptCount())
            .ifPresent(a -> {
                if (a.getStartedAt() == null) {
                    a.setStartedAt(Instant.now());
                    attemptRepository.save(a);
                }
            });
        return job;
    }

    /** Marks a job SUCCEEDED, records the attempt, emits a completion event and metrics. */
    @Transactional
    public JobEntity complete(String jobId) {
        JobEntity job = get(jobId);
        transition(job, JobStatus.SUCCEEDED, "Dataproc execution completed");
        completeAttempt(job, JobStatus.SUCCEEDED, null, null);
        metrics.jobSucceeded(job.getJobType());
        recordDuration(job);
        outboxService.recordTerminal(job, "SUCCEEDED");
        return job;
    }

    /** Marks a job RETRY_WAIT with a transient failure and schedules the next retry. */
    @Transactional
    public JobEntity failRetryable(String jobId, FailureType errorType, String error, Instant nextRetryAt) {
        JobEntity job = get(jobId);
        job.setLastErrorType(errorType);
        job.setLastError(error);
        job.setNextRetryAt(nextRetryAt);
        transition(job, JobStatus.RETRY_WAIT, "Transient failure; retry scheduled");
        completeAttempt(job, JobStatus.FAILED, errorType, error);
        metrics.jobRetried(job.getJobType());
        return job;
    }

    /** Marks a job permanently FAILED, emits a failure event and raises an alert. */
    @Transactional
    public JobEntity failPermanent(String jobId, FailureType errorType, String error) {
        JobEntity job = get(jobId);
        job.setLastErrorType(errorType);
        job.setLastError(error);
        transition(job, JobStatus.FAILED, "Permanent failure");
        completeAttempt(job, JobStatus.FAILED, errorType, error);
        metrics.jobFailed(job.getJobType());
        outboxService.recordTerminal(job, "FAILED");
        alertService.alert(AlertBuilder.build(job, AlertSeverity.CRITICAL, "PERMANENT_FAILURE", error));
        return job;
    }

    /** Marks a job TIMED_OUT (terminal), emits a failure event and raises an alert. */
    @Transactional
    public JobEntity markTimedOut(String jobId, String error) {
        JobEntity job = get(jobId);
        job.setLastErrorType(FailureType.TIMEOUT);
        job.setLastError(error);
        transition(job, JobStatus.TIMED_OUT, "Execution timed out");
        completeAttempt(job, JobStatus.TIMED_OUT, FailureType.TIMEOUT, error);
        outboxService.recordTerminal(job, "TIMED_OUT");
        alertService.alert(AlertBuilder.build(job, AlertSeverity.CRITICAL, "TIMEOUT", error));
        return job;
    }

    /** Requests cancellation of a running/dispatching job (confirmed later by the monitor). */
    @Transactional
    public JobEntity requestCancel(String jobId) {
        JobEntity job = get(jobId);
        JobStatus status = job.getStatus();
        if (status == JobStatus.QUEUED || status == JobStatus.RECEIVED || status == JobStatus.RETRY_WAIT) {
            transition(job, JobStatus.CANCELLED, "Cancelled while queued");
            return job;
        }
        if (status == JobStatus.DISPATCHING || status == JobStatus.SUBMITTED || status == JobStatus.RUNNING) {
            transition(job, JobStatus.CANCEL_REQUESTED, "Cancellation requested");
            return job;
        }
        throw new JobConflictException("Cannot cancel job in state " + status);
    }

    /** Confirms cancellation after Dataproc acknowledges it. */
    @Transactional
    public JobEntity markCancelled(String jobId) {
        JobEntity job = get(jobId);
        transition(job, JobStatus.CANCELLED, "Cancellation confirmed");
        completeAttempt(job, JobStatus.CANCELLED, null, null);
        return job;
    }

    /** Re-queues a FAILED or TIMED_OUT job (manual operator retry). */
    @Transactional
    public JobEntity retryManually(String jobId) {
        JobEntity job = get(jobId);
        if (job.getStatus() != JobStatus.FAILED && job.getStatus() != JobStatus.TIMED_OUT) {
            throw new JobConflictException("Only FAILED or TIMED_OUT jobs can be retried");
        }
        job.setNextRetryAt(null);
        job.setLastError(null);
        job.setLastErrorType(null);
        transition(job, JobStatus.QUEUED, "Manual retry requested");
        return job;
    }

    /**
     * Records a detected execution timeout and requests Dataproc cancellation. Resolution
     * (retry vs terminal TIMED_OUT) happens once cancellation is confirmed by the monitor.
     */
    @Transactional
    public JobEntity requestTimeoutCancel(String jobId, String error) {
        JobEntity job = get(jobId);
        job.setLastErrorType(FailureType.TIMEOUT);
        job.setLastError(error);
        transition(job, JobStatus.CANCEL_REQUESTED, "Execution timed out; cancellation requested");
        return job;
    }

    /** Re-queues a RETRY_WAIT job whose backoff has elapsed. */
    @Transactional
    public JobEntity requeueForRetry(String jobId) {
        JobEntity job = get(jobId);
        job.setNextRetryAt(null);
        transition(job, JobStatus.QUEUED, "Retry backoff elapsed");
        return job;
    }

    /** Reverts a stuck DISPATCHING job back to QUEUED during reconciliation. */
    @Transactional
    public JobEntity revertToQueued(String jobId) {
        JobEntity job = get(jobId);
        transition(job, JobStatus.QUEUED, "Recovered from interrupted dispatch");
        return job;
    }

    /** Applies a monitored Dataproc state synchronisation without a full transition. */
    @Transactional
    public JobEntity syncDataprocJobId(String jobId, String dataprocJobId) {
        JobEntity job = get(jobId);
        job.setDataprocJobId(dataprocJobId);
        return jobRepository.save(job);
    }

    /** Core state transition: validates, stamps, persists and writes history + metrics. */
    private JobEntity transition(JobEntity job, JobStatus to, String reason) {
        JobStatus from = job.getStatus();
        JobStateMachine.assertValid(from, to);
        Instant now = Instant.now();
        switch (to) {
            case QUEUED -> job.setQueuedAt(now);
            case DISPATCHING -> job.setDispatchedAt(now);
            case SUBMITTED -> job.setSubmittedAt(now);
            case RUNNING -> job.setStartedAt(now);
            case SUCCEEDED, FAILED, CANCELLED, TIMED_OUT -> job.setCompletedAt(now);
            default -> { }
        }
        job.setStatus(to);
        job = jobRepository.save(job);
        recordHistory(job, from, to, reason);

        boolean fromHolding = from.isSlotHolding();
        boolean toHolding = to.isSlotHolding();
        if (!fromHolding && toHolding) {
            metrics.slotAcquired(job.getJobType());
        } else if (fromHolding && !toHolding) {
            metrics.slotReleased(job.getJobType());
        }
        if (to == JobStatus.QUEUED) {
            metrics.jobQueued(job.getJobType());
        }
        if (from == JobStatus.QUEUED) {
            metrics.jobDequeued(job.getJobType());
        }
        return job;
    }

    private void completeAttempt(JobEntity job, JobStatus status, FailureType errorType, String error) {
        attemptRepository.findByJobIdAndAttemptNumber(job.getJobId(), job.getAttemptCount())
            .ifPresent(a -> {
                a.setStatus(status);
                a.setCompletedAt(Instant.now());
                if (errorType != null) {
                    a.setErrorType(errorType);
                    a.setErrorMessage(error);
                }
                attemptRepository.save(a);
            });
    }

    private void recordDuration(JobEntity job) {
        Instant start = job.getStartedAt() != null ? job.getStartedAt() : job.getSubmittedAt();
        if (start != null) {
            double seconds = Duration.between(start, Instant.now()).toMillis() / 1000.0;
            metrics.executionDuration(job.getJobType(), seconds);
        }
    }

    private void recordHistory(JobEntity job, JobStatus from, JobStatus to, String reason) {
        historyRepository.save(JobStatusHistoryEntity.builder()
            .jobId(job.getJobId())
            .fromStatus(from)
            .toStatus(to)
            .reason(reason)
            .correlationId(job.getCorrelationId())
            .occurredAt(Instant.now())
            .build());
    }

    private JobEntity get(String jobId) {
        return jobRepository.findByJobId(jobId)
            .orElseThrow(() -> new JobNotFoundException(jobId));
    }

    /** Small builder to keep alert construction readable. */
    private static final class AlertBuilder {
        static Alert build(JobEntity job, AlertSeverity severity, String type, String reason) {
            return new Alert(severity, type, job.getJobId(), job.getJobType(),
                job.getCorrelationId(), job.getDataprocJobId(), reason, job.getAttemptCount());
        }
    }
}
