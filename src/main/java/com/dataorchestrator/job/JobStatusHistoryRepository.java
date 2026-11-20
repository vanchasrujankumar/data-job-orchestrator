package com.dataorchestrator.job;

import java.util.List;

import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * Persistence for the {@link JobStatusHistoryEntity} audit trail.
 */
public interface JobStatusHistoryRepository extends MongoRepository<JobStatusHistoryEntity, String> {

    List<JobStatusHistoryEntity> findByJobIdOrderByOccurredAtAsc(String jobId);
}
