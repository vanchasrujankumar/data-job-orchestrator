package com.dataorchestrator.job;

import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.dataorchestrator.common.JobNotFoundException;

/**
 * Read-side queries for the REST API: filtered/paginated job search, single-job lookup and
 * execution history.
 */
@Service
public class JobQueryService {

    private final JobRepository jobRepository;
    private final ExecutionAttemptRepository attemptRepository;

    public JobQueryService(JobRepository jobRepository, ExecutionAttemptRepository attemptRepository) {
        this.jobRepository = jobRepository;
        this.attemptRepository = attemptRepository;
    }

    @Transactional(readOnly = true)
    public Page<JobEntity> search(JobStatus status, String jobType, Pageable pageable) {
        if (status != null && jobType != null && !jobType.isBlank()) {
            return jobRepository.findByStatusAndJobType(status, jobType, pageable);
        }
        if (status != null) {
            return jobRepository.findByStatus(status, pageable);
        }
        if (jobType != null && !jobType.isBlank()) {
            return jobRepository.findByJobType(jobType, pageable);
        }
        return jobRepository.findAll(pageable);
    }

    @Transactional(readOnly = true)
    public JobEntity get(String jobId) {
        return jobRepository.findByJobId(jobId)
            .orElseThrow(() -> new JobNotFoundException(jobId));
    }

    @Transactional(readOnly = true)
    public List<ExecutionAttemptEntity> executions(String jobId) {
        // Ensure the job exists before listing attempts.
        get(jobId);
        return attemptRepository.findByJobIdOrderByAttemptNumberAsc(jobId);
    }
}
