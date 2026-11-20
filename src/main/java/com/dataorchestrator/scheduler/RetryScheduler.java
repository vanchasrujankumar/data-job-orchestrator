package com.dataorchestrator.scheduler;

import java.time.Instant;
import java.util.List;

import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.dataorchestrator.config.OrchestratorProperties;
import com.dataorchestrator.job.JobEntity;
import com.dataorchestrator.job.JobRepository;
import com.dataorchestrator.job.JobService;
import com.dataorchestrator.job.JobStatus;

/**
 * Re-queues jobs whose retry backoff has elapsed. Re-queued jobs flow through the normal
 * dispatcher so retries respect concurrency limits and priority ordering.
 */
@Component
public class RetryScheduler {

    private final JobRepository jobRepository;
    private final JobService jobService;
    private final SchedulerControlService schedulerControl;
    private final OrchestratorProperties props;

    public RetryScheduler(JobRepository jobRepository, JobService jobService,
                          SchedulerControlService schedulerControl, OrchestratorProperties props) {
        this.jobRepository = jobRepository;
        this.jobService = jobService;
        this.schedulerControl = schedulerControl;
        this.props = props;
    }

    @Scheduled(fixedDelayString = "${orchestrator.scheduling.retry-interval}")
    public void retry() {
        if (schedulerControl.isPaused()) {
            return;
        }
        List<JobEntity> due = jobRepository.findByStatusAndNextRetryAtLessThanEqual(
            JobStatus.RETRY_WAIT, Instant.now(),
            PageRequest.of(0, Math.max(1, props.concurrency().dispatchBatchSize())));
        for (JobEntity job : due) {
            jobService.requeueForRetry(job.getJobId());
        }
    }
}
