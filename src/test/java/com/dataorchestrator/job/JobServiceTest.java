package com.dataorchestrator.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.dataorchestrator.alert.AlertService;
import com.dataorchestrator.common.JobConflictException;
import com.dataorchestrator.config.OrchestratorProperties;
import com.dataorchestrator.metrics.OrchestratorMetrics;
import com.dataorchestrator.outbox.OutboxService;
import com.dataorchestrator.support.TestFixtures;

/**
 * Unit tests for the core job write-path service.
 */
@ExtendWith(MockitoExtension.class)
class JobServiceTest {

    @Mock
    private JobRepository jobRepository;
    @Mock
    private ExecutionAttemptRepository attemptRepository;
    @Mock
    private JobStatusHistoryRepository historyRepository;
    @Mock
    private MongoTemplate mongoTemplate;
    @Mock
    private OrchestratorMetrics metrics;
    @Mock
    private OutboxService outboxService;
    @Mock
    private AlertService alertService;

    private final OrchestratorProperties props = TestFixtures.props();

    private JobService jobService;

    @BeforeEach
    void setUp() {
        jobService = new JobService(jobRepository, attemptRepository, historyRepository,
            mongoTemplate, metrics, outboxService, alertService, props);
    }

    private JobEntity job(JobStatus status, int attempts) {
        return JobEntity.builder()
            .id("id-1")
            .jobId("job-1")
            .eventId("evt-1")
            .jobType("SEGMENT_PREVIEW")
            .priority(JobPriority.NORMAL)
            .status(status)
            .attemptCount(attempts)
            .maxAttempts(3)
            .executionTimeoutSeconds(3600)
            .receivedAt(Instant.now())
            .build();
    }

    @Test
    void claimUsesFindAndModify() {
        JobEntity claimed = job(JobStatus.DISPATCHING, 0);
        when(mongoTemplate.findAndModify(any(Query.class), any(Update.class),
            any(FindAndModifyOptions.class), any())).thenReturn(claimed);

        Optional<JobEntity> result = jobService.claim("id-1", "owner", Instant.now().plusSeconds(60));

        assertThat(result).isPresent();
        verify(mongoTemplate).findAndModify(any(Query.class), any(Update.class),
            any(FindAndModifyOptions.class), any());
        verify(metrics).slotAcquired("SEGMENT_PREVIEW");
    }

    @Test
    void claimReturnsEmptyWhenAlreadyClaimed() {
        when(mongoTemplate.findAndModify(any(Query.class), any(Update.class),
            any(FindAndModifyOptions.class), any())).thenReturn(null);

        Optional<JobEntity> result = jobService.claim("id-1", "owner", Instant.now().plusSeconds(60));

        assertThat(result).isEmpty();
        verify(metrics, never()).slotAcquired(anyString());
    }

    @Test
    void permanentFailureEmitsOutboxAndAlert() {
        JobEntity job = job(JobStatus.RUNNING, 3);
        when(jobRepository.findByJobId("job-1")).thenReturn(Optional.of(job));
        when(jobRepository.save(any(JobEntity.class))).thenReturn(job);

        jobService.failPermanent("job-1", FailureType.PERMANENT, "boom");

        assertThat(job.getStatus()).isEqualTo(JobStatus.FAILED);
        verify(outboxService).recordTerminal(any(JobEntity.class), org.mockito.ArgumentMatchers.eq("FAILED"));
        verify(alertService).alert(any());
    }

    @Test
    void manualRetryOnlyForTerminalFailureStates() {
        JobEntity running = job(JobStatus.RUNNING, 1);
        when(jobRepository.findByJobId("job-1")).thenReturn(Optional.of(running));

        assertThatThrownBy(() -> jobService.retryManually("job-1"))
            .isInstanceOf(JobConflictException.class);
    }

    @Test
    void requeueForRetryClearsNextRetryAt() {
        JobEntity retryWait = job(JobStatus.RETRY_WAIT, 1);
        retryWait.setNextRetryAt(Instant.now());
        when(jobRepository.findByJobId("job-1")).thenReturn(Optional.of(retryWait));
        when(jobRepository.save(any(JobEntity.class))).thenReturn(retryWait);

        jobService.requeueForRetry("job-1");

        assertThat(retryWait.getStatus()).isEqualTo(JobStatus.QUEUED);
        assertThat(retryWait.getNextRetryAt()).isNull();
    }
}
