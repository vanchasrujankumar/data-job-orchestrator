package com.dataorchestrator.scheduler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.dataorchestrator.alert.AlertService;
import com.dataorchestrator.config.OrchestratorProperties;
import com.dataorchestrator.dataproc.DataprocClient;
import com.dataorchestrator.dataproc.DataprocState;
import com.dataorchestrator.dataproc.DataprocStatus;
import com.dataorchestrator.job.JobEntity;
import com.dataorchestrator.job.JobPriority;
import com.dataorchestrator.job.JobRepository;
import com.dataorchestrator.job.JobService;
import com.dataorchestrator.job.JobStatus;
import com.dataorchestrator.metrics.OrchestratorMetrics;
import com.dataorchestrator.support.TestFixtures;

/**
 * Unit tests for Dataproc status monitoring.
 */
@ExtendWith(MockitoExtension.class)
class JobMonitorTest {

    @Mock
    private JobRepository jobRepository;
    @Mock
    private JobService jobService;
    @Mock
    private DataprocClient dataprocClient;
    @Mock
    private OrchestratorMetrics metrics;
    @Mock
    private AlertService alertService;

    private final OrchestratorProperties props = TestFixtures.props();

    private JobMonitor monitor;

    @BeforeEach
    void setUp() {
        monitor = new JobMonitor(jobRepository, jobService, dataprocClient, props, metrics, alertService);
    }

    private JobEntity runningJob(int attempts) {
        return JobEntity.builder()
            .jobId("job-1")
            .eventId("evt-1")
            .jobType("SEGMENT_PREVIEW")
            .priority(JobPriority.NORMAL)
            .status(JobStatus.RUNNING)
            .attemptCount(attempts)
            .maxAttempts(3)
            .dataprocJobId("dataproc-1")
            .executionTimeoutSeconds(3600)
            .submittedAt(Instant.now())
            .build();
    }

    @Test
    void successfulExecutionCompletesJob() {
        JobEntity job = runningJob(1);
        when(jobRepository.findByStatusIn(any())).thenReturn(List.of(job));
        when(dataprocClient.getStatus("dataproc-1", null, null))
            .thenReturn(new DataprocStatus(DataprocState.SUCCEEDED, Instant.now(), null));

        monitor.monitor();

        verify(jobService).complete("job-1");
    }

    @Test
    void failedExecutionIsRetriedWhenAttemptsRemain() {
        JobEntity job = runningJob(1);
        when(jobRepository.findByStatusIn(any())).thenReturn(List.of(job));
        when(dataprocClient.getStatus("dataproc-1", null, null))
            .thenReturn(new DataprocStatus(DataprocState.FAILED, Instant.now(), "crashed"));

        monitor.monitor();

        verify(jobService).failRetryable(eq("job-1"), any(), eq("crashed"), any());
    }

    @Test
    void timeoutRequestsCancellation() {
        JobEntity job = runningJob(1);
        job.setExecutionTimeoutSeconds(10);
        job.setSubmittedAt(Instant.now().minusSeconds(100));
        when(jobRepository.findByStatusIn(any())).thenReturn(List.of(job));
        when(dataprocClient.getStatus("dataproc-1", null, null))
            .thenReturn(new DataprocStatus(DataprocState.RUNNING, Instant.now(), null));

        monitor.monitor();

        verify(jobService).requestTimeoutCancel(eq("job-1"), anyString());
    }

    @Test
    void runningStateDoesNotCompleteJob() {
        JobEntity job = runningJob(1);
        when(jobRepository.findByStatusIn(any())).thenReturn(List.of(job));
        when(dataprocClient.getStatus("dataproc-1", null, null))
            .thenReturn(new DataprocStatus(DataprocState.RUNNING, Instant.now(), null));

        monitor.monitor();

        verify(jobService, never()).complete(anyString());
    }
}
