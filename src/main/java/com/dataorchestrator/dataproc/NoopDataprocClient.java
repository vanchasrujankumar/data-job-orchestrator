package com.dataorchestrator.dataproc;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Local/development Dataproc client. Simulates an immediately-succeeding execution so the
 * full lifecycle can be exercised without a real GCP project. Not for production use.
 */
@Component
@ConditionalOnProperty(name = "orchestrator.dataproc.provider", havingValue = "noop", matchIfMissing = true)
public class NoopDataprocClient implements DataprocClient {

    private static final Logger log = LoggerFactory.getLogger(NoopDataprocClient.class);

    @Override
    public DataprocSubmitResult submit(DataprocSubmitRequest request) {
        String id = "noop-" + request.jobId() + "-" + request.attemptNumber();
        log.info("Noop Dataproc submit jobId={} dataprocJobId={}", request.jobId(), id);
        return new DataprocSubmitResult(id, DataprocState.SUCCEEDED);
    }

    @Override
    public DataprocStatus getStatus(String dataprocJobId, String projectId, String region) {
        return new DataprocStatus(DataprocState.SUCCEEDED, Instant.now(), null);
    }

    @Override
    public void cancel(String dataprocJobId, String projectId, String region) {
        log.info("Noop Dataproc cancel dataprocJobId={}", dataprocJobId);
    }
}
