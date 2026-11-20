package com.dataorchestrator.scheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.dataorchestrator.alert.Alert;
import com.dataorchestrator.alert.AlertService;
import com.dataorchestrator.alert.AlertSeverity;
import com.dataorchestrator.config.OrchestratorProperties;
import com.dataorchestrator.dataproc.DataprocClient;
import com.dataorchestrator.dataproc.DataprocState;
import com.dataorchestrator.dataproc.DataprocStatus;
import com.dataorchestrator.job.FailureType;
import com.dataorchestrator.job.JobEntity;
import com.dataorchestrator.job.JobRepository;
import com.dataorchestrator.job.JobService;
import com.dataorchestrator.job.JobStatus;
import com.dataorchestrator.job.RetryPolicy;
import com.dataorchestrator.metrics.OrchestratorMetrics;

/**
 * Periodically polls Dataproc for the state of active executions, synchronises the database,
 * detects timeouts and resolves cancellations. Temporary API failures are logged and retried
 * on the next cycle rather than being mistaken for execution failure.
 */
@Component
public class JobMonitor {

    private static final Logger log = LoggerFactory.getLogger(JobMonitor.class);

    private final JobRepository jobRepository;
    private final JobService jobService;
    private final DataprocClient dataprocClient;
    private final OrchestratorProperties props;
    private final OrchestratorMetrics metrics;
    private final AlertService alertService;

    public JobMonitor(JobRepository jobRepository, JobService jobService,
                      DataprocClient dataprocClient, OrchestratorProperties props,
                      OrchestratorMetrics metrics, AlertService alertService) {
        this.jobRepository = jobRepository;
        this.jobService = jobService;
        this.dataprocClient = dataprocClient;
        this.props = props;
        this.metrics = metrics;
        this.alertService = alertService;
    }

    @Scheduled(fixedDelayString = "${orchestrator.scheduling.monitor-interval}")
    public void monitor() {
        List<JobEntity> active = jobRepository.findByStatusIn(
            List.of(JobStatus.SUBMITTED, JobStatus.RUNNING, JobStatus.CANCEL_REQUESTED));
        for (JobEntity job : active) {
            try {
                monitorOne(job);
            } catch (Exception e) {
                // A transient monitoring error must not mark the job as failed.
                log.warn("Monitoring attempt failed jobId={} reason={}", job.getJobId(), e.getMessage());
            }
        }
    }

    private void monitorOne(JobEntity job) {
        if (job.getStatus() == JobStatus.CANCEL_REQUESTED) {
            resolveCancellation(job);
            return;
        }

        DataprocStatus status = fetchStatus(job);
        switch (status.state()) {
            case SUCCEEDED -> jobService.complete(job.getJobId());
            case FAILED -> resolveFailure(job, status.detail() == null ? "Dataproc execution failed" : status.detail());
            case CANCELLED -> jobService.markCancelled(job.getJobId());
            case RUNNING, PENDING -> {
                if (job.getStatus() == JobStatus.SUBMITTED) {
                    jobService.markRunning(job.getJobId());
                }
                checkTimeout(job);
            }
            default -> log.warn("Unknown Dataproc state jobId={} state={}",
                job.getJobId(), status.state());
        }
    }

    private void resolveCancellation(JobEntity job) {
        safeCancel(job);
        DataprocStatus status = fetchStatus(job);
        if (status.state() == DataprocState.CANCELLED) {
            if (job.getLastErrorType() == FailureType.TIMEOUT) {
                resolveTimeout(job);
            } else {
                jobService.markCancelled(job.getJobId());
            }
        } else if (status.state() == DataprocState.FAILED) {
            resolveFailure(job, "Cancellation resulted in failure");
        }
        // otherwise still cancelling; wait for the next cycle
    }

    private void resolveFailure(JobEntity job, String error) {
        if (job.getAttemptCount() < job.getMaxAttempts()) {
            Instant next = Instant.now().plus(RetryPolicy.computeBackoff(job.getAttemptCount(), props.retry()));
            jobService.failRetryable(job.getJobId(), FailureType.TRANSIENT, error, next);
        } else {
            jobService.failPermanent(job.getJobId(), FailureType.TRANSIENT, error);
        }
    }

    private void resolveTimeout(JobEntity job) {
        String error = job.getLastError() == null ? "Execution timed out" : job.getLastError();
        if (job.getAttemptCount() < job.getMaxAttempts()) {
            Instant next = Instant.now().plus(RetryPolicy.computeBackoff(job.getAttemptCount(), props.retry()));
            jobService.failRetryable(job.getJobId(), FailureType.TIMEOUT, error, next);
        } else {
            jobService.markTimedOut(job.getJobId(), error);
        }
    }

    private void checkTimeout(JobEntity job) {
        Instant start = job.getSubmittedAt() != null ? job.getSubmittedAt() : job.getDispatchedAt();
        if (start == null) {
            return;
        }
        Duration limit = Duration.ofSeconds(job.getExecutionTimeoutSeconds());
        if (Duration.between(start, Instant.now()).compareTo(limit) > 0) {
            log.warn("Execution timeout detected jobId={}", job.getJobId());
            alertService.alert(new Alert(AlertSeverity.WARNING, "EXECUTION_TIMEOUT", job.getJobId(),
                job.getJobType(), job.getCorrelationId(), job.getDataprocJobId(),
                "Execution exceeded " + limit.toSeconds() + "s", job.getAttemptCount()));
            jobService.requestTimeoutCancel(job.getJobId(), "Execution exceeded "
                + limit.toSeconds() + "s");
            safeCancel(job);
        }
    }

    private DataprocStatus fetchStatus(JobEntity job) {
        try {
            return dataprocClient.getStatus(job.getDataprocJobId(),
                job.getDataprocProjectId(), job.getDataprocRegion());
        } catch (Exception e) {
            metrics.dataprocApiFailure(job.getJobType());
            throw e;
        }
    }

    private void safeCancel(JobEntity job) {
        try {
            dataprocClient.cancel(job.getDataprocJobId(), job.getDataprocProjectId(), job.getDataprocRegion());
        } catch (Exception e) {
            metrics.dataprocApiFailure(job.getJobType());
            log.warn("Cancel request failed jobId={} reason={}", job.getJobId(), e.getMessage());
        }
    }
}
