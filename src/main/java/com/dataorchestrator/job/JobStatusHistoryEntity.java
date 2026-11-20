package com.dataorchestrator.job;

import java.time.Instant;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * MongoDB document forming the append-only audit trail of every logical status change a job
 * goes through.
 */
@Document(collection = "job_status_history")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class JobStatusHistoryEntity {

    @Id
    private String id;

    @Indexed
    private String jobId;

    private JobStatus fromStatus;

    private JobStatus toStatus;

    private String reason;

    private String correlationId;

    private Instant occurredAt;
}
