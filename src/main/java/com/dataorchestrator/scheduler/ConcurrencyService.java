package com.dataorchestrator.scheduler;

import java.util.Set;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.dataorchestrator.config.OrchestratorProperties;
import com.dataorchestrator.job.JobRepository;
import com.dataorchestrator.job.JobStatus;

/**
 * Tracks concurrency reservation against configured limits. A slot is reserved for any job
 * in a slot-holding state (DISPATCHING, SUBMITTED, RUNNING, CANCEL_REQUESTED); the database
 * is the single source of truth so limits apply across all replicas.
 */
@Service
public class ConcurrencyService {

    private static final Set<JobStatus> RESERVED =
        Set.of(JobStatus.DISPATCHING, JobStatus.SUBMITTED, JobStatus.RUNNING, JobStatus.CANCEL_REQUESTED);

    private final JobRepository jobRepository;
    private final OrchestratorProperties props;

    public ConcurrencyService(JobRepository jobRepository, OrchestratorProperties props) {
        this.jobRepository = jobRepository;
        this.props = props;
    }

    /** Number of currently reserved slots for a job type. */
    @Transactional(readOnly = true)
    public int reservedSlots(String jobType) {
        return (int) jobRepository.countByStatusInAndJobType(RESERVED, jobType);
    }

    /** Total number of reserved slots across all job types. */
    @Transactional(readOnly = true)
    public int reservedSlotsGlobal() {
        return (int) jobRepository.countByStatusIn(RESERVED);
    }

    /** Number of queued jobs for a job type. */
    @Transactional(readOnly = true)
    public int queuedJobs(String jobType) {
        return (int) jobRepository.countByStatusInAndJobType(Set.of(JobStatus.QUEUED), jobType);
    }

    /** True if another job of the given type may be dispatched now. */
    @Transactional(readOnly = true)
    public boolean hasCapacity(String jobType) {
        return reservedSlotsGlobal() < props.concurrency().globalMaxJobs()
            && reservedSlots(jobType) < props.concurrencyLimit(jobType);
    }
}
