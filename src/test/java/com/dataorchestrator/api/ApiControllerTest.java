package com.dataorchestrator.api;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import com.dataorchestrator.common.JobNotFoundException;
import com.dataorchestrator.job.JobEntity;
import com.dataorchestrator.job.JobPriority;
import com.dataorchestrator.job.JobQueryService;
import com.dataorchestrator.job.JobService;
import com.dataorchestrator.job.JobStatus;
import com.dataorchestrator.scheduler.SchedulerControlService;
import com.dataorchestrator.scheduler.SchedulerStatus;

/**
 * Slice tests for the REST API.
 */
@WebMvcTest(controllers = {JobController.class, SchedulerController.class})
class ApiControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private JobQueryService queryService;

    @MockitoBean
    private JobService jobService;

    @MockitoBean
    private SchedulerControlService schedulerControlService;

    @Test
    void getJobReturnsDetails() throws Exception {
        JobEntity job = JobEntity.builder()
            .jobId("job-1")
            .jobType("SEGMENT_PREVIEW")
            .priority(JobPriority.HIGH)
            .status(JobStatus.RUNNING)
            .attemptCount(1)
            .maxAttempts(3)
            .correlationId("corr-1")
            .receivedAt(Instant.now())
            .build();
        when(queryService.get("job-1")).thenReturn(job);

        mockMvc.perform(get("/api/v1/jobs/job-1"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.jobId").value("job-1"))
            .andExpect(jsonPath("$.status").value("RUNNING"));
    }

    @Test
    void getUnknownJobReturns404() throws Exception {
        when(queryService.get("missing")).thenThrow(new JobNotFoundException("missing"));

        mockMvc.perform(get("/api/v1/jobs/missing"))
            .andExpect(status().isNotFound());
    }

    @Test
    void retryReturnsUpdatedJob() throws Exception {
        JobEntity job = JobEntity.builder()
            .jobId("job-1")
            .jobType("SEGMENT_PREVIEW")
            .priority(JobPriority.NORMAL)
            .status(JobStatus.QUEUED)
            .attemptCount(1)
            .maxAttempts(3)
            .receivedAt(Instant.now())
            .build();
        when(jobService.retryManually("job-1")).thenReturn(job);

        mockMvc.perform(post("/api/v1/jobs/job-1/retry"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("QUEUED"));
    }

    @Test
    void schedulerStatusReturnsOk() throws Exception {
        when(schedulerControlService.status()).thenReturn(
            new SchedulerStatus(false, 10, 2, java.util.Map.of()));

        mockMvc.perform(get("/api/v1/scheduler/status"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.paused").value(false))
            .andExpect(jsonPath("$.globalMaxJobs").value(10));
    }

    @Test
    void pauseReturnsStatus() throws Exception {
        when(schedulerControlService.pause()).thenReturn(
            new SchedulerStatus(true, 10, 2, java.util.Map.of()));

        mockMvc.perform(post("/api/v1/scheduler/pause"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.paused").value(true));
    }
}
