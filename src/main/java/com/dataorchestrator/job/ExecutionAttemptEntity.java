package com.dataorchestrator.job;

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
 * MongoDB document for a single physical execution attempt of a logical job. Each retry
 * creates a new document so the full execution history is preserved independently of the
 * parent {@link JobEntity}.
 */
@Document(collection = "job_execution_attempts")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ExecutionAttemptEntity {

    @Id
    private String id;

    /** Parent logical job id (not the Mongo document id). */
    @Indexed
    private String jobId;

    /** 1-based attempt number. */
    private int attemptNumber;

    private JobStatus status;

    /** Dataproc execution identifier for this attempt. */
    private String dataprocJobId;

    private FailureType errorType;

    private String errorMessage;

    private Instant submittedAt;

    private Instant startedAt;

    private Instant completedAt;

    @CreatedDate
    private Instant createdAt;
}
