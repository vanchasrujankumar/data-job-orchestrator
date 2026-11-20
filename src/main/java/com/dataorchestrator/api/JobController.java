package com.dataorchestrator.api;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.dataorchestrator.api.dto.ExecutionResponse;
import com.dataorchestrator.api.dto.JobResponse;
import com.dataorchestrator.api.dto.PageResponse;
import com.dataorchestrator.job.JobQueryService;
import com.dataorchestrator.job.JobService;
import com.dataorchestrator.job.JobStatus;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

/**
 * REST endpoints for retrieving jobs and performing operational actions (retry, cancel).
 */
@RestController
@RequestMapping("/api/v1/jobs")
@Tag(name = "Jobs", description = "Job retrieval and operational actions")
public class JobController {

    private final JobQueryService queryService;
    private final JobService jobService;

    public JobController(JobQueryService queryService, JobService jobService) {
        this.queryService = queryService;
        this.jobService = jobService;
    }

    @GetMapping
    @Operation(summary = "List jobs", description = "Filter by status and/or job type, paginated.")
    public PageResponse<JobResponse> list(
            @RequestParam(required = false) JobStatus status,
            @RequestParam(required = false) String jobType,
            @PageableDefault(size = 20, sort = "createdAt") Pageable pageable) {
        return PageResponse.of(queryService.search(status, jobType, pageable), JobResponse::from);
    }

    @GetMapping("/{jobId}")
    @Operation(summary = "Get a job", description = "Retrieve full detail for a single job.")
    public JobResponse get(@PathVariable String jobId) {
        return JobResponse.from(queryService.get(jobId));
    }

    @GetMapping("/{jobId}/executions")
    @Operation(summary = "List execution attempts", description = "Execution history for a job.")
    public List<ExecutionResponse> executions(@PathVariable String jobId) {
        return queryService.executions(jobId).stream().map(ExecutionResponse::from).toList();
    }

    @PostMapping("/{jobId}/retry")
    @Operation(summary = "Retry a job", description = "Manually re-queue a FAILED or TIMED_OUT job.")
    public JobResponse retry(@PathVariable String jobId) {
        return JobResponse.from(jobService.retryManually(jobId));
    }

    @PostMapping("/{jobId}/cancel")
    @Operation(summary = "Cancel a job", description = "Cancel a queued or running job.")
    public JobResponse cancel(@PathVariable String jobId) {
        return JobResponse.from(jobService.requestCancel(jobId));
    }
}
