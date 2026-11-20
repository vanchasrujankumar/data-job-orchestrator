package com.dataorchestrator.dataproc;

import java.io.IOException;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.dataorchestrator.config.OrchestratorProperties;
import com.google.cloud.dataproc.v1.CancelJobRequest;
import com.google.cloud.dataproc.v1.GetJobRequest;
import com.google.cloud.dataproc.v1.Job;
import com.google.cloud.dataproc.v1.JobControllerClient;
import com.google.cloud.dataproc.v1.JobControllerSettings;
import com.google.cloud.dataproc.v1.JobPlacement;
import com.google.cloud.dataproc.v1.JobStatus;
import com.google.cloud.dataproc.v1.SparkJob;
import com.google.cloud.dataproc.v1.SubmitJobRequest;
import com.google.protobuf.Timestamp;

/**
 * Production Dataproc client backed by the official Google Cloud Dataproc Java SDK
 * ({@code com.google.cloud:google-cloud-dataproc}). Uses Application Default Credentials
 * for authentication and supports an optional custom endpoint override.
 */
@Component
@ConditionalOnProperty(name = "orchestrator.dataproc.provider", havingValue = "gcp")
public class GoogleDataprocClient implements DataprocClient {

    private static final Logger log = LoggerFactory.getLogger(GoogleDataprocClient.class);

    private static final String DEFAULT_ENDPOINT = "dataproc.googleapis.com:443";

    private final OrchestratorProperties.Dataproc config;
    private final JobControllerClient client;

    public GoogleDataprocClient(OrchestratorProperties props) {
        this.config = props.dataproc();
        String endpoint = (config.endpoint() == null || config.endpoint().isBlank())
            ? DEFAULT_ENDPOINT : config.endpoint();
        try {
            JobControllerSettings settings = JobControllerSettings.newBuilder()
                .setEndpoint(endpoint)
                .build();
            this.client = JobControllerClient.create(settings);
            log.info("GoogleDataprocClient initialised (endpoint={})", endpoint);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to initialise Dataproc client", e);
        }
    }

    @Override
    public DataprocSubmitResult submit(DataprocSubmitRequest request) {
        String projectId = resolve(request.projectId(), config.projectId());
        String region = resolve(request.region(), config.region());
        String cluster = resolve(request.spec().clusterName(), config.clusterName());

        SparkJob.Builder spark = SparkJob.newBuilder()
            .setMainClass(request.spec().mainClass())
            .addAllJarFileUris(request.spec().jarFileUris())
            .addAllArgs(request.spec().args());
        request.spec().properties().forEach(spark::putProperties);

        Job job = Job.newBuilder()
            .setPlacement(JobPlacement.newBuilder().setClusterName(cluster).build())
            .setSparkJob(spark.build())
            .build();

        SubmitJobRequest submitRequest = SubmitJobRequest.newBuilder()
            .setProjectId(projectId)
            .setRegion(region)
            .setJob(job)
            .setRequestId(request.requestId())
            .build();

        try {
            Job submitted = client.submitJob(submitRequest);
            log.info("Dataproc submit accepted jobId={} dataprocJobId={}",
                request.jobId(), submitted.getReference().getJobId());
            return new DataprocSubmitResult(submitted.getReference().getJobId(),
                mapState(submitted.getStatus()));
        } catch (Exception e) {
            throw new DataprocException("Dataproc submission failed for job " + request.jobId(), e);
        }
    }

    @Override
    public DataprocStatus getStatus(String dataprocJobId, String projectId, String region) {
        GetJobRequest request = GetJobRequest.newBuilder()
            .setProjectId(resolve(projectId, config.projectId()))
            .setRegion(resolve(region, config.region()))
            .setJobId(dataprocJobId)
            .build();
        try {
            Job job = client.getJob(request);
            return new DataprocStatus(mapState(job.getStatus()),
                toInstant(job.getStatus().getStateStartTime()),
                job.getStatus().getDetails());
        } catch (Exception e) {
            throw new DataprocException("Dataproc status lookup failed for " + dataprocJobId, e);
        }
    }

    @Override
    public void cancel(String dataprocJobId, String projectId, String region) {
        CancelJobRequest request = CancelJobRequest.newBuilder()
            .setProjectId(resolve(projectId, config.projectId()))
            .setRegion(resolve(region, config.region()))
            .setJobId(dataprocJobId)
            .build();
        try {
            client.cancelJob(request);
            log.info("Dataproc cancel requested dataprocJobId={}", dataprocJobId);
        } catch (Exception e) {
            throw new DataprocException("Dataproc cancel failed for " + dataprocJobId, e);
        }
    }

    private String resolve(String specific, String fallback) {
        return (specific == null || specific.isBlank()) ? fallback : specific;
    }

    private Instant toInstant(Timestamp ts) {
        return ts == null ? null : Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
    }

    private DataprocState mapState(JobStatus status) {
        if (status == null) {
            return DataprocState.UNKNOWN;
        }
        return switch (status.getState()) {
            case DONE -> DataprocState.SUCCEEDED;
            case ERROR, ATTEMPT_FAILURE -> DataprocState.FAILED;
            case CANCELLED -> DataprocState.CANCELLED;
            case RUNNING, PENDING, SETUP_DONE, CANCEL_PENDING, CANCEL_STARTED -> DataprocState.RUNNING;
            default -> DataprocState.UNKNOWN;
        };
    }
}
