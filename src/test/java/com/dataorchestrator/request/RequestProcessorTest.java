package com.dataorchestrator.request;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.dataorchestrator.common.JobValidationException;
import com.dataorchestrator.job.JobEntity;
import com.dataorchestrator.job.JobRepository;
import com.dataorchestrator.job.JobService;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.validation.Validation;
import jakarta.validation.Validator;

/**
 * Unit tests for request parsing, validation and deduplication.
 */
@ExtendWith(MockitoExtension.class)
class RequestProcessorTest {

    @Mock
    private JobRepository jobRepository;

    @Mock
    private JobService jobService;

    private RequestProcessor processor;

    @BeforeEach
    void setUp() {
        ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
        processor = new RequestProcessor(objectMapper, validator, jobRepository, jobService);
    }

    private String validJson() {
        return """
            {
              "eventId": "evt-1",
              "jobId": "job-1",
              "jobType": "SEGMENT_PREVIEW",
              "priority": "HIGH",
              "executionParameters": {
                "mainClass": "com.example.SparkApp",
                "jarFileUris": ["gs://bucket/app.jar"],
                "args": ["--date=2026-01-01"],
                "properties": {"spark.executor.memory": "2g"}
              }
            }
            """;
    }

    @Test
    void validPayloadIsAccepted() {
        JobEntity job = JobEntity.builder().jobId("job-1").eventId("evt-1").build();
        when(jobRepository.existsByEventId("evt-1")).thenReturn(false);
        when(jobService.accept(anyString(), anyString(), anyString(), anyString(), any(), anyString()))
            .thenReturn(job);

        ProcessingOutcome outcome = processor.process(validJson());

        assertThat(outcome.duplicate()).isFalse();
        assertThat(outcome.job().getJobId()).isEqualTo("job-1");
        verify(jobService).accept(anyString(), anyString(), anyString(), anyString(), any(), anyString());
    }

    @Test
    void duplicateEventIsIgnored() {
        JobEntity existing = JobEntity.builder().jobId("job-1").eventId("evt-1").build();
        when(jobRepository.existsByEventId("evt-1")).thenReturn(true);
        when(jobRepository.findByEventId("evt-1")).thenReturn(Optional.of(existing));

        ProcessingOutcome outcome = processor.process(validJson());

        assertThat(outcome.duplicate()).isTrue();
        verify(jobService, never()).accept(anyString(), anyString(), anyString(), anyString(), any(), anyString());
    }

    @Test
    void malformedJsonIsRejected() {
        assertThatThrownBy(() -> processor.process("{not json"))
            .isInstanceOf(JobValidationException.class);
    }

    @Test
    void missingJobTypeIsRejected() {
        String json = """
            {"eventId": "evt-2", "executionParameters": {
                "mainClass": "c", "jarFileUris": ["j"], "args": [], "properties": {}
            }}
            """;
        assertThatThrownBy(() -> processor.process(json))
            .isInstanceOf(JobValidationException.class);
    }

    @Test
    void missingExecutionParametersIsRejected() {
        String json = """
            {"eventId": "evt-3", "jobType": "SEGMENT_PREVIEW"}
            """;
        assertThatThrownBy(() -> processor.process(json))
            .isInstanceOf(JobValidationException.class);
    }
}
