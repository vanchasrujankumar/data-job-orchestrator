# AGENTS.md — Data Job Orchestration Service

Project memory for AI agents. Read this before making changes to this repo.

## What this is

A centralized, event-driven **Spring Boot** service that consumes Dataproc job execution
requests from **Kafka**, persists and validates them in **MongoDB**, submits Spark workloads
to **Google Cloud Dataproc** under configurable concurrency limits, tracks the full job
lifecycle, retries transient failures, alerts on permanent failures, and publishes
completion/failure events back to Kafka (transactional outbox).

## Tech stack (non-negotiable, user-chosen)

| Concern | Choice | Notes |
|---|---|---|
| Language | Java 21 | toolchain in `build.gradle` |
| Framework | Spring Boot **3.5.7** | **not 4.x** — pinned because start.spring.io only offers 4.x now |
| Build | Gradle **8.14.3** wrapper | **not Maven** — user explicitly requested Gradle |
| Database | **MongoDB** (Spring Data MongoDB) | **not PostgreSQL** — user explicitly switched |
| Messaging | Apache Kafka | Spring Kafka |
| Execution | Google Cloud Dataproc **Serverless** SDK (`google-cloud-dataproc:4.51.0`) | behind a `DataprocClient` port (`serverless` / `noop`) |
| Observability | **OpenTelemetry** (OTEL 1.50.0) | **not Micrometer** — user explicitly requested OTEL |
| API docs | springdoc-openapi 2.8.9 (Swagger UI) | `/swagger-ui.html` |

## Key build / test / run commands

```bash
./gradlew compileJava        # compile only
./gradlew test               # unit + slice tests (40 tests; 2 Testcontainers auto-skip w/o Docker)
./gradlew bootJar            # build the runnable jar (needed before Docker image build)
./gradlew bootRun            # run locally (expects Mongo+Kafka on localhost)

# Full stack (MongoDB replica set + KRaft Kafka + app) — requires Docker Desktop running
docker compose up -d --build
docker compose down          # stop (+ -v to drop data volumes)
```

Note: `docker build` requires the jar to exist first (`./gradlew bootJar`). On macOS Docker
Desktop, `docker-credential-desktop` may not be on `$PATH` in non-login shells — export
`PATH="$PATH:/Applications/Docker.app/Contents/Resources/bin"` if pulls fail.

## Package layout (`com.dataorchestrator`)

```
api/            REST controllers + DTOs
alert/          AlertService port + LoggingAlertService (pluggable; no real platform)
common/         CorrelationIdFilter, GlobalExceptionHandler (RFC 9457), domain exceptions
config/         OrchestratorProperties, KafkaConfig, MongoConfig, OpenTelemetryConfig, OpenApiConfig
dataproc/       DataprocClient (GoogleDataprocServerlessClient + NoopDataprocClient), SparkJobSpec
job/            MongoDB documents, repositories, JobStateMachine, JobService, RetryPolicy, FailureClassifier
metrics/        OrchestratorMetrics (OTEL Meter)
outbox/         OutboxEventEntity + OutboxPublisher (relay) + OutboxService
request/        JobRequestConsumer, RequestProcessor (validate + dedup), JobRequestPayload
scheduler/      JobDispatcher, JobMonitor, RetryScheduler, ReconciliationService, ConcurrencyService, SchedulerControlService
```

## MongoDB collections

`jobs`, `job_execution_attempts`, `job_status_history`, `outbox_events`, `scheduler_state`.
See `docs/data-model.md`. `spring.data.mongodb.auto-index-creation=true` creates `@Indexed`
indexes at startup. Transactions (`@Transactional` on `JobService`) require the single-node
**replica set** configured in `docker-compose.yml` (healthcheck runs `rs.initiate`).

## Important design invariants (do not break)

1. **Dedup** — `jobs.eventId` has a unique index; duplicates are skipped in `RequestProcessor`.
2. **Cross-replica claim** — `JobService.claim` uses `MongoTemplate.findAndModify` (atomic),
   querying `{_id, status: QUEUED}`. Never use read-then-write for claiming.
3. **Idempotent submission** — Dataproc request id = `jobId-attempt-N` (`DataprocSubmitRequest.requestId()`).
4. **State machine** — all status changes go through `JobService.transition` → `JobStateMachine.assertValid`.
5. **Slot states** — `DISPATCHING`, `SUBMITTED`, `RUNNING`, `CANCEL_REQUESTED` hold a slot.
   `transition` updates the OTEL up-down counters.
6. **Outbox** — `OutboxService.recordTerminal` MUST set `status=PENDING` explicitly (a
   previous `@PrePersist` default was lost in the JPA→Mongo migration and caused a silent
   "events never published" bug — `OutboxServiceTest` guards this).
7. **Metrics cardinality** — never use `jobId` as a metric label; only `jobType`.

## Known gaps (not yet implemented)

- **API authorization/audit logging** (Story 10) — no Spring Security.
- **CI/CD pipeline** (Story 15) — no GitHub Actions/GitLab workflow.
- **Queue-delay + stuck-job alerts** (Story 8) — `alerting.queue-delay-threshold` config
  exists but no alert is generated.
- **`queueWait` metric** — instrument defined in `OrchestratorMetrics` but never recorded.
- **Alert history persistence** — dedup is in-memory (TTL map) only.
- **Failure-injection / restart tests** (Story 15) — reconciliation is not unit-tested.

## Docs

- `README.md` — essentials only.
- `docs/architecture.md` — Mermaid flow, lifecycle state machine, UML.
- `docs/data-model.md` — MongoDB collection structures.
- `docs/setup.md` — local setup + end-to-end test steps.
- `docs/configuration.md` — full config reference.

## Session history

- Initial build: planned first, then implemented all 15 epic stories (foundation → execution
  → reliability → operations → validation).
- Mid-build user decisions: Gradle (was Maven), OTEL (was Micrometer), MongoDB via Docker
  (was PostgreSQL + Flyway/H2), Swagger, Javadocs, Mermaid docs, slim README.
- End-to-end Docker run verified: Kafka → MongoDB → dispatch → Dataproc (noop) → `SUCCEEDED`
  → completion event on `job-events`.
- Final state: committed as `25cc6b2` on `main`; 40 tests pass; Docker stack brought down.
