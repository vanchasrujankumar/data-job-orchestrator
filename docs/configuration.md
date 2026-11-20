# Configuration Reference

All behaviour is environment-driven via the `orchestrator.*` namespace (see
`src/main/resources/application.yml`). Spring Boot's relaxed binding means keys are also
available as uppercase environment variables.

## `orchestrator.kafka`

| Key | Default | Description |
|---|---|---|
| `job-request-topic` | `job-requests` | Topic for incoming job requests |
| `job-events-topic` | `job-events` | Topic for completion/failure events |
| `dead-letter-topic` | `job-requests-dlt` | Topic for invalid/unprocessable messages |
| `consumer-group` | `data-job-orchestrator` | Kafka consumer group |
| `consumer-concurrency` | `3` | Number of consumer threads |

## `orchestrator.concurrency`

| Key | Default | Description |
|---|---|---|
| `global-max-jobs` | `10` | Global maximum concurrent jobs |
| `job-types` | `{}` | Per-job-type concurrency limits (overrides global) |
| `dispatch-batch-size` | `100` | Max queued jobs considered per dispatch cycle |
| `lease-duration` | `60s` | How long a dispatch reservation is valid before reconciliation |

## `orchestrator.retry`

| Key | Default | Description |
|---|---|---|
| `max-attempts` | `3` | Maximum execution attempts (including the first) |
| `initial-delay` | `60s` | Backoff before the first retry |
| `multiplier` | `2` | Exponential backoff multiplier |
| `max-delay` | `900s` | Upper bound on backoff |

## `orchestrator.dataproc`

| Key | Default | Description |
|---|---|---|
| `provider` | `noop` | `noop` (simulated) or `serverless` (Dataproc Serverless/Batch SDK) |
| `project-id` | — | GCP project |
| `region` | `us-central1` | Dataproc region |
| `endpoint` | — | Optional Dataproc endpoint override |

## `orchestrator.spark-jobs`

Defines the Spark workload per job type. The Spark job (main class, jar files and Spark
configuration such as dynamic executor allocation and memory settings) is configured once
here; each Kafka request only supplies the business `jobParameters` passed to the job as a
single JSON argument.

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

| Key | Description |
|---|---|
| `main-class` | Spark main class |
| `jar-file-uris` | Jar files made available to the Spark job |
| `properties` | `spark.*` configuration (executor allocation + memory), applied to the Dataproc Serverless batch runtime |

## `orchestrator.timeout`

| Key | Default | Description |
|---|---|---|
| `default-max-duration` | `1h` | Default max execution duration |
| `by-job-type` | `{}` | Per-job-type max durations |

## `orchestrator.scheduling`

| Key | Default | Description |
|---|---|---|
| `dispatch-interval` | `5s` | Dispatcher poll interval |
| `monitor-interval` | `15s` | Dataproc status poll interval |
| `retry-interval` | `15s` | Retry scheduler poll interval |
| `reconcile-interval` | `60s` | Reconciliation interval |
| `outbox-interval` | `5s` | Outbox relay interval |
| `starvation-threshold` | `5m` | Aging threshold that promotes lower-priority jobs |

## `orchestrator.otel`

| Key | Default | Description |
|---|---|---|
| `enabled` | `true` | Enable OpenTelemetry (set `false` in tests) |
| `service-name` | `data-job-orchestrator` | Service name for resource attributes |
| `otlp-endpoint` | — | OTLP/HTTP endpoint; unset → console/logging exporter |

## Key environment variables

| Variable | Maps to |
|---|---|
| `MONGODB_URI` | `spring.data.mongodb.uri` |
| `KAFKA_BOOTSTRAP_SERVERS` | `spring.kafka.bootstrap-servers` |
| `KAFKA_JOB_REQUEST_TOPIC` | `orchestrator.kafka.job-request-topic` |
| `KAFKA_JOB_EVENTS_TOPIC` | `orchestrator.kafka.job-events-topic` |
| `DATAPROC_PROVIDER` | `orchestrator.dataproc.provider` |
| `GCP_PROJECT_ID` | `orchestrator.dataproc.project-id` |
| `GCP_REGION` | `orchestrator.dataproc.region` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `orchestrator.otel.otlp-endpoint` |
| `OTEL_ENABLED` | `orchestrator.otel.enabled` |

## Logging

Structured JSON logging is enabled with `SPRING_PROFILES_ACTIVE=json`; otherwise a
human-readable pattern is used. Correlation ids are carried in the `correlationId` MDC field.
