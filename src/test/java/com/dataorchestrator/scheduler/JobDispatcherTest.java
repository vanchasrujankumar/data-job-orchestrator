package com.dataorchestrator.scheduler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageImpl;

import com.dataorchestrator.config.InstanceIdentity;
import com.dataorchestrator.config.OrchestratorProperties;
import com.dataorchestrator.dataproc.DataprocClient;
import com.dataorchestrator.dataproc.DataprocSubmitResult;
import com.dataorchestrator.dataproc.DataprocState;
import com.dataorchestrator.job.JobEntity;
import com.dataorchestrator.job.JobPriority;
import com.dataorchestrator.job.JobRepository;
import com.dataorchestrator.job.JobService;
import com.dataorchestrator.job.JobStatus;
import com.dataorchestrator.metrics.OrchestratorMetrics;
import com.dataorchestrator.support.TestFixtures;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Unit tests for the dispatch scheduler: concurrency gating and claim-submit flow.
 */
@ExtendWith(MockitoExtension.class)
class JobDispatcherTest {

    @Mock
    private JobRepository jobRepository;
    @Mock
    private JobService jobService;
    @Mock
    private ConcurrencyService concurrencyService;
    @Mock
    private SchedulerControlService schedulerControl;
    @Mock
    private DataprocClient dataprocClient;
    @Mock
    private OrchestratorMetrics metrics;

    private final OrchestratorProperties props = TestFixtures.props();
    private final InstanceIdentity identity = new InstanceIdentity();

    private JobDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        dispatcher = new JobDispatcher(jobRepository, jobService, concurrencyService,
            schedulerControl, dataprocClient, props, identity, metrics, objectMapper);
    }

    private JobEntity queuedJob() {
        return JobEntity.builder()
            .id("id-1")
            .jobId("job-1")
            .eventId("evt-1")
            .jobType("SEGMENT_PREVIEW")
            .priority(JobPriority.HIGH)
            .status(JobStatus.QUEUED)
            .attemptCount(0)
            .maxAttempts(3)
            .executionConfig("""
                {"mainClass":"c","jarFileUris":["j"],"args":[],"properties":{}}
                """)
            .executionTimeoutSeconds(3600)
            .createdAt(Instant.now())
            .build();
    }

    @Test
    void dispatchesEligibleJob() {
        JobEntity job = queuedJob();
        when(schedulerControl.isPaused()).thenReturn(false);
        when(jobRepository.findByStatus(eq(JobStatus.QUEUED), any()))
            .thenReturn(new PageImpl<>(List.of(job)));
        when(concurrencyService.hasCapacity("SEGMENT_PREVIEW")).thenReturn(true);
        when(jobService.claim(eq("id-1"), anyString(), any())).thenReturn(Optional.of(job));
        when(jobService.beginAttempt("job-1")).thenReturn(1);
        when(dataprocClient.submit(any()))
            .thenReturn(new DataprocSubmitResult("dataproc-1", DataprocState.PENDING));

        dispatcher.dispatch();

        verify(dataprocClient).submit(any());
        verify(jobService).markSubmitted(eq("job-1"), eq("dataproc-1"), anyString(), anyString(), anyString());
    }

    @Test
    void skipsDispatchWhenPaused() {
        when(schedulerControl.isPaused()).thenReturn(true);

        dispatcher.dispatch();

        verify(jobRepository, never()).findByStatus(any(), any());
        verify(jobService, never()).claim(anyString(), anyString(), any());
    }

    @Test
    void skipsDispatchWhenNoCapacity() {
        JobEntity job = queuedJob();
        when(schedulerControl.isPaused()).thenReturn(false);
        when(jobRepository.findByStatus(eq(JobStatus.QUEUED), any()))
            .thenReturn(new PageImpl<>(List.of(job)));
        when(concurrencyService.hasCapacity("SEGMENT_PREVIEW")).thenReturn(false);

        dispatcher.dispatch();

        verify(jobService, never()).claim(anyString(), anyString(), any());
    }
}
