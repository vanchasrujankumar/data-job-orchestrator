package com.dataorchestrator.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.dataorchestrator.config.OrchestratorProperties;
import com.dataorchestrator.job.JobEntity;
import com.dataorchestrator.support.TestFixtures;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Unit tests for the outbox event writer.
 */
@ExtendWith(MockitoExtension.class)
class OutboxServiceTest {

    @Mock
    private OutboxEventRepository repository;

    private final OrchestratorProperties props = TestFixtures.props();

    private OutboxService outboxService;

    @BeforeEach
    void setUp() {
        when(repository.save(any(OutboxEventEntity.class))).thenAnswer(inv -> inv.getArgument(0));
        outboxService = new OutboxService(repository, new ObjectMapper(), props);
    }

    @Test
    void completionEventContainsRequiredMetadata() throws Exception {
        JobEntity job = JobEntity.builder()
            .jobId("job-1")
            .jobType("SEGMENT_PREVIEW")
            .correlationId("corr-1")
            .attemptCount(2)
            .build();

        outboxService.recordTerminal(job, "SUCCEEDED");

        ArgumentCaptor<OutboxEventEntity> captor = ArgumentCaptor.forClass(OutboxEventEntity.class);
        verify(repository).save(captor.capture());
        OutboxEventEntity event = captor.getValue();
        assertThat(event.getEventId()).isEqualTo("completion-job-1");
        assertThat(event.getTopic()).isEqualTo("job-events");
        assertThat(event.getEventType()).isEqualTo("JOB_COMPLETED");
        assertThat(event.getStatus()).isEqualTo(OutboxEventEntity.OutboxStatus.PENDING);

        JsonNode payload = new ObjectMapper().readTree(event.getPayload());
        assertThat(payload.get("jobId").asText()).isEqualTo("job-1");
        assertThat(payload.get("status").asText()).isEqualTo("SUCCEEDED");
        assertThat(payload.get("attemptNumber").asInt()).isEqualTo(2);
    }

    @Test
    void failureEventUsesFailureEventId() {
        JobEntity job = JobEntity.builder()
            .jobId("job-2")
            .jobType("SEGMENT_METRICS")
            .attemptCount(3)
            .build();

        outboxService.recordTerminal(job, "FAILED");

        ArgumentCaptor<OutboxEventEntity> captor = ArgumentCaptor.forClass(OutboxEventEntity.class);
        verify(repository).save(captor.capture());
        assertThat(captor.getValue().getEventId()).isEqualTo("failure-job-2");
        assertThat(captor.getValue().getEventType()).isEqualTo("JOB_FAILED");
    }
}
