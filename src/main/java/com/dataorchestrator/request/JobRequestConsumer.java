package com.dataorchestrator.request;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer for Dataproc job execution requests. Consumes raw JSON strings, delegates
 * to {@link RequestProcessor}, and lets the container commit the offset only after
 * processing succeeds. Consumption is fully decoupled from the (asynchronous) Dataproc
 * execution lifecycle.
 */
@Component
public class JobRequestConsumer {

    private static final Logger log = LoggerFactory.getLogger(JobRequestConsumer.class);
    private static final String CORRELATION_HEADER = "correlationId";

    private final RequestProcessor processor;

    public JobRequestConsumer(RequestProcessor processor) {
        this.processor = processor;
    }

    @KafkaListener(
        topics = "${orchestrator.kafka.job-request-topic}",
        groupId = "${orchestrator.kafka.consumer-group}")
    public void onMessage(ConsumerRecord<String, String> record) {
        String correlationId = headerValue(record, CORRELATION_HEADER);
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = java.util.UUID.randomUUID().toString();
        }
        MDC.put("correlationId", correlationId);
        try {
            ProcessingOutcome outcome = processor.process(record.value());
            if (outcome.duplicate()) {
                log.info("Duplicate event ignored eventId={} jobId={}",
                    outcome.job().getEventId(), outcome.job().getJobId());
            } else {
                log.info("Job accepted jobId={} jobType={} correlationId={}",
                    outcome.job().getJobId(), outcome.job().getJobType(), correlationId);
            }
        } finally {
            MDC.remove("correlationId");
        }
    }

    private String headerValue(ConsumerRecord<String, String> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
