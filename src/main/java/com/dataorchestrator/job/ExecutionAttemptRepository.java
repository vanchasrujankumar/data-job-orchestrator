package com.dataorchestrator.job;

import java.util.List;
import java.util.Optional;

import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * Persistence for {@link ExecutionAttemptEntity}.
 */
public interface ExecutionAttemptRepository extends MongoRepository<ExecutionAttemptEntity, String> {

    List<ExecutionAttemptEntity> findByJobIdOrderByAttemptNumberAsc(String jobId);

    Optional<ExecutionAttemptEntity> findByJobIdAndAttemptNumber(String jobId, int attemptNumber);
}
