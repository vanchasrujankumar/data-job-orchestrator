# Data Job Orchestration Service

A centralized, event-driven **Spring Boot** service that consumes Dataproc job execution
requests from **Kafka**, validates and persists them in **MongoDB**, submits Spark workloads
to **Google Cloud Dataproc** under configurable concurrency limits, tracks their full
lifecycle, retries transient failures, alerts on permanent failures, and publishes results
back to Kafka.

| Area | Choice |
|---|---|
| Language / Runtime | Java 21, Spring Boot 3.5 |
| Build | Gradle (wrapper included) |
| Messaging | Apache Kafka |
| Database | MongoDB (replica set, via Docker) |
| Execution | Google Cloud Dataproc **Serverless** (Spark batches) |
| Observability | OpenTelemetry (metrics + tracing) — no Micrometer |
| API docs | OpenAPI / Swagger UI |

## Prerequisites

- **Docker Desktop** (running) — for MongoDB and Kafka.
- **Java 21** — only needed for the "run the app locally" option below.

## Run it

**Option A — everything in Docker (simplest):**

```bash
docker compose up -d --build   # MongoDB (replica set) + Kafka (KRaft) + the app
```

**Option B — app on the host, dependencies in Docker:**

```bash
docker compose up -d mongodb kafka   # start only the dependencies
./gradlew bootRun                    # run the app locally
```

The default config already targets `localhost:27017` (MongoDB) and `localhost:9092` (Kafka),
so no extra environment variables are needed for a local run.

After startup:

- Swagger UI — http://localhost:8080/swagger-ui.html
- Health — http://localhost:8080/actuator/health
- Metrics — http://localhost:8080/actuator/metrics

## Execute a job end-to-end

1. Start the stack (Option A or B) and wait until health returns `UP`.
2. Publish a job request to the `job-requests` topic:

   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-console-producer.sh \
     --bootstrap-server localhost:9092 --topic job-requests
   ```

   Paste this message, then press `Ctrl+D`. Only business data is sent over Kafka — the
   Spark job itself is configured per job type (see step 2b below):

   ```json
   {
     "eventId": "evt-1001",
     "jobId": "job-1001",
     "jobType": "SEGMENT_PREVIEW",
     "correlationId": "seg-1",
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
   argument. The Spark workload itself is defined in `orchestrator.spark-jobs` (in
   `application.yml` / env), e.g.:

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

   Equivalent to running:

   ```bash
   spark-submit --class com.example.SegmentPreviewApp \
     --conf spark.dynamicAllocation.enabled=true \
     --conf spark.dynamicAllocation.minExecutors=2 \
     --conf spark.dynamicAllocation.maxExecutors=20 \
     --conf spark.executor.memory=4g \
     --conf spark.executor.cores=2 \
     --conf spark.executor.memoryOverhead=1g \
     --conf spark.driver.memory=2g \
     gs://my-bucket/app.jar '{"previewMessage":"...","segmentCriteria":{...}}'
   ```

3. Verify the job was accepted and completed:

   ```bash
   curl http://localhost:8080/api/v1/jobs/job-1001
   curl http://localhost:8080/api/v1/jobs/job-1001/executions
   curl http://localhost:8080/api/v1/scheduler/status
   ```

4. Watch the completion event published to `job-events`:

   ```bash
   docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 --topic job-events --from-beginning --max-messages 1
   ```

With the default `DATAPROC_PROVIDER=noop` the job goes straight to `SUCCEEDED`. To run against
real Dataproc **Serverless**, set `DATAPROC_PROVIDER=serverless` (plus `GCP_PROJECT_ID` and
`GCP_REGION`) — see [setup](docs/setup.md).

Stop everything with `docker compose down` (add `-v` to also drop the data volumes).

## What it does

- Consumes, validates, and de-duplicates Kafka job requests.
- Enforces global and per-job-type concurrency limits across replicas.
- Submits and monitors Dataproc jobs asynchronously.
- Retries transient failures with exponential backoff and jitter.
- Alerts on permanent failures, timeouts, and stuck jobs.
- Publishes completion/failure events via a transactional outbox.
- Recovers interrupted dispatches after restarts.
- Exposes operational REST APIs and OpenTelemetry metrics.

## Documentation

| Doc | Contents |
|---|---|
| [docs/setup.md](docs/setup.md) | Local setup, configuration, and end-to-end testing |
| [docs/architecture.md](docs/architecture.md) | Architecture, lifecycle state machine, UML, design rationale |
| [docs/data-model.md](docs/data-model.md) | MongoDB collection structures |
| [docs/configuration.md](docs/configuration.md) | Configuration reference and environment variables |

## Out of scope

Cost optimisation, multi-region routing, resource-based scheduling, BigQuery integration,
and DAG-based workflow orchestration.
