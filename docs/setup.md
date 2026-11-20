# Local Setup

## Prerequisites

- Java 21
- Docker Desktop (running) — for MongoDB and Kafka
- Gradle is **not** required; the wrapper (`./gradlew`) is included.

## Option A — run everything in Docker

```bash
docker compose up -d --build
```

This starts MongoDB (single-node replica set), Kafka (KRaft), and the application.

| Endpoint | URL |
|---|---|
| Swagger UI | http://localhost:8080/swagger-ui.html |
| OpenAPI JSON | http://localhost:8080/v3/api-docs |
| Health | http://localhost:8080/actuator/health |
| Metrics | http://localhost:8080/actuator/metrics |

Stop with `docker compose down` (add `-v` to also remove data volumes).

## Option B — run the app locally, Docker only for dependencies

```bash
# 1. Start MongoDB + Kafka only
docker compose up -d mongodb kafka

# 2. Run the app from the host
./gradlew bootRun
```

The default configuration already points at `localhost:27017` and `localhost:9092`, so no
extra environment variables are needed for a local run with the `noop` Dataproc provider.

## End-to-end test

1. Start the stack (Option A or B) and wait for the health endpoint to return `UP`.
2. Publish a job request to the `job-requests` topic:

   ```bash
   docker compose exec kafka \
     /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic job-requests
   ```

   Then paste this message and press Enter (press Ctrl+D to finish). Only business data is
   sent over Kafka — the Spark job is configured per job type (see step 2b):

   ```json
   {
     "eventId": "evt-1001",
     "jobId": "job-1001",
     "jobType": "SEGMENT_PREVIEW",
     "correlationId": "segment-0001",
     "priority": "HIGH",
     "jobParameters": {
       "previewMessage": "New segment preview request",
       "segmentCriteria": {
         "segmentId": "seg-42",
         "startDate": "2026-11-01",
         "endDate": "2026-11-19"
       }
     }
   }
   ```

   The `jobParameters` object is serialized to JSON and passed to the Spark job as its single
   argument. The Spark workload itself is defined in `orchestrator.spark-jobs` (see
   [configuration.md](configuration.md) and `application.yml`), e.g.:

   ```yaml
   orchestrator:
     spark-jobs:
       SEGMENT_PREVIEW:
         main-class: com.example.SegmentPreviewApp
         jar-file-uris: [ gs://my-bucket/app.jar ]
         properties:
           spark.dynamicAllocation.enabled: "true"
           spark.dynamicAllocation.minExecutors: "2"
           spark.dynamicAllocation.maxExecutors: "20"
           spark.dynamicAllocation.initialExecutors: "2"
           spark.executor.memory: "4g"
           spark.executor.cores: "2"
           spark.executor.memoryOverhead: "1g"
           spark.driver.memory: "2g"
   ```

3. Verify the job was accepted and completed:

   ```bash
   # Job status
   curl http://localhost:8080/api/v1/jobs/job-1001
   # Execution history
   curl http://localhost:8080/api/v1/jobs/job-1001/executions
   ```

   With the `noop` provider the job transitions straight to `SUCCEEDED` and a completion event
   is published to the `job-events` topic:

   ```bash
   docker compose exec kafka \
     kafka-console-consumer.sh --bootstrap-server localhost:9092 \
     --topic job-events --from-beginning --max-messages 1
   ```

4. Scheduler status:

   ```bash
   curl http://localhost:8080/api/v1/scheduler/status
   ```

## Running the tests

```bash
./gradlew test        # unit + slice tests (no Docker needed)
./gradlew build       # full build (tests + jar)
```

The Testcontainers-based MongoDB integration test runs automatically when Docker is available
and is skipped otherwise.

## Using real Google Cloud Dataproc

```bash
# Authenticate with Application Default Credentials
gcloud auth application-default login
```

**Dataproc Serverless:**

```bash
export DATAPROC_PROVIDER=serverless
export GCP_PROJECT_ID=my-project
export GCP_REGION=us-central1
```

Submissions use the Dataproc Serverless `BatchControllerClient`; each job is a Spark batch
and no cluster needs to exist. Jobs are submitted asynchronously (non-blocking).

Then run the app; submissions use the official Dataproc Java SDK.

