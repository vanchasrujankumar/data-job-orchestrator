package com.dataorchestrator.outbox;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.dataorchestrator.outbox.OutboxEventEntity.OutboxStatus;

/**
 * Relays {@link OutboxEventEntity} rows to Kafka. Runs on a fixed delay, publishes pending
 * events and marks them {@code PUBLISHED} on success. A failed publication stays
 * {@code PENDING} so it is retried on the next cycle — restart-safe, at-least-once.
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final int BATCH_SIZE = 100;

    private final OutboxEventRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxPublisher(OutboxEventRepository repository, KafkaTemplate<String, String> kafkaTemplate) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${orchestrator.scheduling.outbox-interval}")
    public void publish() {
        List<OutboxEventEntity> pending =
            repository.findByStatus(OutboxStatus.PENDING, PageRequest.of(0, BATCH_SIZE));
        for (OutboxEventEntity event : pending) {
            try {
                kafkaTemplate.send(event.getTopic(), event.getEventId(), event.getPayload())
                    .get(10, TimeUnit.SECONDS);
                event.setStatus(OutboxStatus.PUBLISHED);
                event.setPublishedAt(Instant.now());
                repository.save(event);
            } catch (Exception e) {
                log.warn("Outbox publish failed eventId={} topic={} reason={}",
                    event.getEventId(), event.getTopic(), e.getMessage());
                // Leave PENDING for the next cycle; delivery is retried indefinitely.
            }
        }
    }
}
