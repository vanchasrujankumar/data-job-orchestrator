package com.dataorchestrator.scheduler;

import java.time.Instant;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.dataorchestrator.job.JobEntity;
import com.dataorchestrator.job.JobRepository;
import com.dataorchestrator.job.JobService;
import com.dataorchestrator.job.JobStatus;

/**
 * Recovers jobs left in an inconsistent state by crashes, restarts or network failures.
 *
 * <p>Stale DISPATCHING jobs (whose lease expired) are reverted to QUEUED. Re-dispatch is
 * safe because Dataproc submission uses a deterministic request id ({@code jobId-attempt-N}),
 * so a re-submitted job is idempotent and never duplicates an execution.</p>
 */
@Component
public class ReconciliationService {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationService.class);

    private final JobRepository jobRepository;
    private final JobService jobService;

    public ReconciliationService(JobRepository jobRepository, JobService jobService) {
        this.jobRepository = jobRepository;
        this.jobService = jobService;
    }

    /** Runs once on startup so recovery happens before normal dispatch resumes. */
    @EventListener(ApplicationReadyEvent.class)
    public void onStartup() {
        reconcile();
    }

    @Scheduled(fixedDelayString = "${orchestrator.scheduling.reconcile-interval}")
    public void reconcile() {
        List<JobEntity> stale = jobRepository.findByStatusAndLeaseExpiresAtBefore(
            JobStatus.DISPATCHING, Instant.now());
        for (JobEntity job : stale) {
            log.warn("Reconciling stale DISPATCHING job jobId={} leaseOwner={}",
                job.getJobId(), job.getLeaseOwner());
            jobService.revertToQueued(job.getJobId());
        }
    }
}
