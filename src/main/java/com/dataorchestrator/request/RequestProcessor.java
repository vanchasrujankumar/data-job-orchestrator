package com.dataorchestrator.request;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import com.dataorchestrator.common.JobValidationException;
import com.dataorchestrator.job.JobEntity;
import com.dataorchestrator.job.JobPriority;
import com.dataorchestrator.job.JobRepository;
import com.dataorchestrator.job.JobService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;

/**
 * Parses, validates and persists incoming Kafka job requests. Deduplicates on the
 * {@code eventId} unique constraint so a repeated delivery never creates a second logical
 * job. Invalid payloads throw {@link JobValidationException}, which the Kafka error handler
 * routes to the dead-letter topic.
 */
@Service
public class RequestProcessor {

    private final ObjectMapper objectMapper;
    private final Validator validator;
    private final JobRepository jobRepository;
    private final JobService jobService;

    public RequestProcessor(ObjectMapper objectMapper, Validator validator,
                            JobRepository jobRepository, JobService jobService) {
        this.objectMapper = objectMapper;
        this.validator = validator;
        this.jobRepository = jobRepository;
        this.jobService = jobService;
    }

    /**
     * Processes one raw JSON message. Returns the job (existing on duplicate, otherwise the
     * newly created and queued job).
     */
    public ProcessingOutcome process(String json) {
        JobRequestPayload payload = parse(json);
        validate(payload);

        if (jobRepository.existsByEventId(payload.eventId())) {
            JobEntity existing = jobRepository.findByEventId(payload.eventId()).orElseThrow();
            return new ProcessingOutcome(existing, true);
        }

        String jobId = payload.jobId() == null || payload.jobId().isBlank()
            ? UUID.randomUUID().toString() : payload.jobId();
        String correlationId = payload.correlationId() == null || payload.correlationId().isBlank()
            ? UUID.randomUUID().toString() : payload.correlationId();
        JobPriority priority = payload.priority() == null ? JobPriority.NORMAL : payload.priority();
        String configJson = toJson(payload.executionParameters());

        JobEntity job = jobService.accept(payload.eventId(), jobId, correlationId,
            payload.jobType(), priority, configJson);
        return new ProcessingOutcome(job, false);
    }

    private JobRequestPayload parse(String json) {
        try {
            return objectMapper.readValue(json, JobRequestPayload.class);
        } catch (JsonProcessingException e) {
            throw new JobValidationException("Malformed JSON payload: " + e.getOriginalMessage());
        }
    }

    private void validate(JobRequestPayload payload) {
        Set<ConstraintViolation<JobRequestPayload>> violations = validator.validate(payload);
        if (!violations.isEmpty()) {
            String message = violations.stream()
                .map(v -> v.getPropertyPath() + " " + v.getMessage())
                .collect(Collectors.joining("; "));
            throw new JobValidationException("Validation failed: " + message);
        }
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new JobValidationException("Failed to serialize execution parameters");
        }
    }
}
