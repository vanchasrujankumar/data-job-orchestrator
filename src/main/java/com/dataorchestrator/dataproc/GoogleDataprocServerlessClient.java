package com.dataorchestrator.dataproc;

import java.io.IOException;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.dataorchestrator.config.OrchestratorProperties;
import com.google.cloud.dataproc.v1.Batch;
import com.google.cloud.dataproc.v1.BatchControllerClient;
import com.google.cloud.dataproc.v1.BatchControllerSettings;
import com.google.cloud.dataproc.v1.CreateBatchRequest;
import com.google.cloud.dataproc.v1.DeleteBatchRequest;
import com.google.cloud.dataproc.v1.GetBatchRequest;
import com.google.cloud.dataproc.v1.RuntimeConfig;
import com.google.cloud.dataproc.v1.SparkBatch;
import com.google.protobuf.Timestamp;

/**
 * Dataproc <b>Serverless</b> client backed by the official Dataproc Java SDK
 * ({@code BatchControllerClient}). Submits Spark jobs as batches — no cluster provisioning.
 *
 * <p>Submission is non-blocking: a deterministic {@code batchId} is supplied
 * ({@code jobId-attempt-N}) and the long-running operation is not awaited, so the Kafka
 * consumer and dispatcher are never blocked by Spark execution. Retrying with the same
 * {@code batchId} is idempotent.</p>
 */
@Component
@ConditionalOnProperty(name = "orchestrator.dataproc.provider", havingValue = "serverless")
public class GoogleDataprocServerlessClient implements DataprocClient {

    private static final Logger log = LoggerFactory.getLogger(GoogleDataprocServerlessClient.class);

    private static final String DEFAULT_ENDPOINT = "dataproc.googleapis.com:443";

    private final OrchestratorProperties.Dataproc config;
    private final BatchControllerClient client;

    public GoogleDataprocServerlessClient(OrchestratorProperties props) {
        this.config = props.dataproc();
        String endpoint = (config.endpoint() == null || config.endpoint().isBlank())
            ? DEFAULT_ENDPOINT : config.endpoint();
        try {
            BatchControllerSettings settings = BatchControllerSettings.newBuilder()
                .setEndpoint(endpoint)
                .build();
            this.client = BatchControllerClient.create(settings);
            log.info("GoogleDataprocServerlessClient initialised (endpoint={})", endpoint);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to initialise Dataproc Serverless client", e);
        }
    }

    @Override
    public DataprocSubmitResult submit(DataprocSubmitRequest request) {
        String projectId = resolve(request.projectId(), config.projectId());
        String region = resolve(request.region(), config.region());
        String batchId = sanitizeBatchId(request.requestId());

        SparkBatch spark = SparkBatch.newBuilder()
            .setMainClass(request.spec().mainClass())
            .addAllJarFileUris(request.spec().jarFileUris())
            .addAllArgs(request.spec().args())
            .build();

        RuntimeConfig runtime = RuntimeConfig.newBuilder()
            .putAllProperties(request.spec().properties())
            .build();

        Batch batch = Batch.newBuilder()
            .setSparkBatch(spark)
            .setRuntimeConfig(runtime)
            .build();

        CreateBatchRequest createRequest = CreateBatchRequest.newBuilder()
            .setParent("projects/" + projectId + "/locations/" + region)
            .setBatch(batch)
            .setBatchId(batchId)
            .setRequestId(request.requestId())
            .build();

        try {
            // Fire the create RPC but do NOT await the long-running operation (which
            // completes only when the Spark batch finishes). The deterministic batchId is
            // returned immediately; the monitor polls it via getStatus.
            var operation = client.createBatchAsync(createRequest);
            log.info("Dataproc Serverless batch submitted jobId={} batchId={} operation={}",
                request.jobId(), batchId, operation.getName());
            return new DataprocSubmitResult(batchId, DataprocState.PENDING);
        } catch (Exception e) {
            throw new DataprocException("Dataproc Serverless submission failed for job "
                + request.jobId(), e);
        }
    }

    @Override
    public DataprocStatus getStatus(String batchId, String projectId, String region) {
        GetBatchRequest request = GetBatchRequest.newBuilder()
            .setName(batchName(batchId, projectId, region))
            .build();
        try {
            Batch batch = client.getBatch(request);
            return new DataprocStatus(mapState(batch.getState()),
                toInstant(batch.getStateTime()), batch.getStateMessage());
        } catch (Exception e) {
            throw new DataprocException("Dataproc Serverless status lookup failed for "
                + batchId, e);
        }
    }

    @Override
    public void cancel(String batchId, String projectId, String region) {
        DeleteBatchRequest request = DeleteBatchRequest.newBuilder()
            .setName(batchName(batchId, projectId, region))
            .build();
        try {
            client.deleteBatch(request);
            log.info("Dataproc Serverless batch delete requested batchId={}", batchId);
        } catch (Exception e) {
            throw new DataprocException("Dataproc Serverless cancel failed for " + batchId, e);
        }
    }

    private String batchName(String batchId, String projectId, String region) {
        return "projects/" + resolve(projectId, config.projectId())
            + "/locations/" + resolve(region, config.region())
            + "/batches/" + batchId;
    }

    private String resolve(String specific, String fallback) {
        return (specific == null || specific.isBlank()) ? fallback : specific;
    }

    /** Dataproc batch ids must be lowercase alphanumeric + hyphens, max 63 chars. */
    private String sanitizeBatchId(String raw) {
        String id = raw.toLowerCase()
            .replaceAll("[^a-z0-9-]", "-")
            .replaceAll("-{2,}", "-");
        if (!id.matches("^[a-z].*")) {
            id = "b-" + id;
        }
        if (id.length() > 63) {
            id = id.substring(0, 63).replaceAll("-$", "");
        }
        return id;
    }

    private Instant toInstant(Timestamp ts) {
        return ts == null ? null : Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
    }

    private DataprocState mapState(Batch.State state) {
        if (state == null) {
            return DataprocState.UNKNOWN;
        }
        return switch (state) {
            case SUCCEEDED -> DataprocState.SUCCEEDED;
            case FAILED -> DataprocState.FAILED;
            case CANCELLED -> DataprocState.CANCELLED;
            case PENDING, RUNNING, CANCELLING -> DataprocState.RUNNING;
            default -> DataprocState.UNKNOWN;
        };
    }
}
