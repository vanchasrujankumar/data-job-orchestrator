package com.dataorchestrator.outbox;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * Persistence for the transactional outbox.
 */
public interface OutboxEventRepository extends MongoRepository<OutboxEventEntity, String> {

    List<OutboxEventEntity> findByStatus(OutboxEventEntity.OutboxStatus status, Pageable pageable);
}
