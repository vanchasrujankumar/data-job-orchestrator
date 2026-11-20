package com.dataorchestrator.outbox;

import java.util.Map;

import org.springframework.stereotype.Component;

import com.dataorchestrator.config.OrchestratorProperties;
import com.dataorchestrator.job.JobEntity;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Writes a terminal job event to the transactional outbox in the same database transaction
 * as the job state change. The event is later relayed to Kafka by {@link OutboxPublisher}.
 */
@Component
public class OutboxService {

    private final OutboxEventRepository repository;
    private final ObjectMapper objectMapper;
    private final OrchestratorProperties props;

    public OutboxService(OutboxEventRepository repository, ObjectMapper objectMapper,
                         OrchestratorProperties props) {
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.props = props;
    }

    /** Records a terminal event (SUCCEEDED, FAILED or TIMED_OUT) for downstream consumers. */
    public void recordTerminal(JobEntity job, String status) {
        boolean success = "SUCCEEDED".equals(status);
        String eventType = success ? "JOB_COMPLETED" : "JOB_FAILED";
        String eventId = (success ? "completion-" : "failure-") + job.getJobId();

        Map<String, Object> payload = Map.of(
            "eventId", eventId,
            "jobId", job.getJobId(),
            "jobType", job.getJobType(),
            "status", status,
            "attemptNumber", job.getAttemptCount(),
            "correlationId", job.getCorrelationId() == null ? "" : job.getCorrelationId()
        );

        OutboxEventEntity event = OutboxEventEntity.builder()
            .eventId(eventId)
            .jobId(job.getJobId())
            .eventType(eventType)
            .topic(props.kafka().jobEventsTopic())
            .payload(toJson(payload))
            .correlationId(job.getCorrelationId())
            .status(OutboxEventEntity.OutboxStatus.PENDING)
            .build();
        repository.save(event);
    }

    private String toJson(Map<String, Object> payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            // Should never happen for this simple map; fall back to a manual serialization.
            throw new IllegalStateException("Failed to serialize outbox payload", e);
        }
    }
}
