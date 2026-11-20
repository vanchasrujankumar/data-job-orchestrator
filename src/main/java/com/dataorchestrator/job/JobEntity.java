package com.dataorchestrator.job;

import java.time.Instant;

import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * MongoDB document representing a logical job. One document per accepted job request,
 * independent of the number of physical Dataproc execution attempts. It is the anchor for
 * execution history, retries, concurrency reservation and recovery.
 */
@Document(collection = "jobs")
@CompoundIndexes({
    @CompoundIndex(name = "idx_status_priority_created", def = "{'status': 1, 'priority': 1, 'createdAt': 1}"),
    @CompoundIndex(name = "idx_type_status", def = "{'jobType': 1, 'status': 1}")
})
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobEntity {

    @Id
    private String id;

    /** Business identifier; unique across the service. */
    @Indexed(unique = true)
    private String jobId;

    /** Kafka event id used for idempotent deduplication. */
    @Indexed(unique = true)
    private String eventId;

    /** Correlation id for end-to-end traceability (propagated or generated). */
    private String correlationId;

    @Indexed
    private String jobType;

    private JobPriority priority;

    @Indexed
    private JobStatus status;

    /** Number of execution attempts performed so far. */
    private int attemptCount;

    /** Resolved maximum attempts for this job (defaults from retry configuration). */
    private int maxAttempts;

    private Instant nextRetryAt;

    private FailureType lastErrorType;

    private String lastError;

    /** Latest Dataproc execution identifier (physical job/batch id). */
    private String dataprocJobId;

    private String dataprocProjectId;

    private String dataprocRegion;

    /** Serialized business parameters (JSON) passed to the Spark job as an argument. */
    private String jobParameters;

    private int executionTimeoutSeconds;

    /** Replica identifier holding the current dispatch reservation (for reconciliation). */
    private String leaseOwner;

    private Instant leaseExpiresAt;

    private Instant receivedAt;

    private Instant queuedAt;

    private Instant dispatchedAt;

    private Instant submittedAt;

    private Instant startedAt;

    private Instant completedAt;

    @CreatedDate
    private Instant createdAt;

    @LastModifiedDate
    private Instant updatedAt;
}
