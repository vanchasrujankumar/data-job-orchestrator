package com.dataorchestrator.job;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * Persistence for {@link JobEntity}. The atomic claim (reserve a queued job exactly once
 * across replicas) is implemented in {@link JobService} using {@code findAndModify}.
 */
public interface JobRepository extends MongoRepository<JobEntity, String> {

    Optional<JobEntity> findByJobId(String jobId);

    boolean existsByEventId(String eventId);

    Optional<JobEntity> findByEventId(String eventId);

    Page<JobEntity> findByStatus(JobStatus status, Pageable pageable);

    Page<JobEntity> findByStatusAndJobType(JobStatus status, String jobType, Pageable pageable);

    Page<JobEntity> findByJobType(String jobType, Pageable pageable);

    List<JobEntity> findByStatusIn(Collection<JobStatus> statuses);

    List<JobEntity> findByStatusAndNextRetryAtLessThanEqual(JobStatus status, Instant now, Pageable pageable);

    List<JobEntity> findByStatusAndLeaseExpiresAtBefore(JobStatus status, Instant now);

    long countByStatusIn(Collection<JobStatus> statuses);

    long countByStatusInAndJobType(Collection<JobStatus> statuses, String jobType);
}
