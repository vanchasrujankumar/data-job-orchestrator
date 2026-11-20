package com.dataorchestrator.scheduler;

import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * Persistence for the singleton {@link SchedulerStateEntity} control document.
 */
public interface SchedulerStateRepository extends MongoRepository<SchedulerStateEntity, String> {
}
