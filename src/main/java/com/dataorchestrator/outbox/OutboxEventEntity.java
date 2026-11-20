package com.dataorchestrator.outbox;

import java.time.Instant;

import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * MongoDB document for the transactional outbox. A completion/failure event is written here
 * in the same transaction as the job state change, then asynchronously relayed to Kafka by
 * {@link OutboxPublisher}. This guarantees at-least-once publication with no lost events on
 * restart.
 */
@Document(collection = "outbox_events")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxEventEntity {

    public enum OutboxStatus {
        PENDING,
        PUBLISHED,
        FAILED
    }

    @Id
    private String id;

    /** Unique event id used by downstream consumers for deduplication. */
    @Indexed(unique = true)
    private String eventId;

    /** Logical job id this event refers to. */
    private String jobId;

    private String eventType;

    private String topic;

    /** JSON payload published to Kafka. */
    private String payload;

    private String correlationId;

    @Indexed
    private OutboxStatus status;

    @CreatedDate
    private Instant createdAt;

    private Instant publishedAt;
}
