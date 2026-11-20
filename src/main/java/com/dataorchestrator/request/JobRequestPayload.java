package com.dataorchestrator.request;

import com.dataorchestrator.dataproc.SparkJobSpec;
import com.dataorchestrator.job.JobPriority;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * Incoming Kafka job-request payload. {@code eventId} is the deduplication key;
 * {@code jobId} and {@code correlationId} are optional and generated when absent so the
 * service always has a unique logical id and a trace id.
 */
public record JobRequestPayload(
    @NotBlank(message = "eventId is required") String eventId,
    String jobId,
    @NotBlank(message = "jobType is required") String jobType,
    String correlationId,
    JobPriority priority,
    @Valid @NotNull(message = "executionParameters is required") SparkJobSpec executionParameters
) {
}
