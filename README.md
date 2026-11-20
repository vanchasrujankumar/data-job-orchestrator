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

## Quick start

```bash
docker compose up -d --build   # MongoDB + Kafka + the app
```

Then open:

- Swagger UI — http://localhost:8080/swagger-ui.html
- Health — http://localhost:8080/actuator/health
- Metrics — http://localhost:8080/actuator/metrics

The app starts with `DATAPROC_PROVIDER=noop` (simulated execution) so the full lifecycle
works without GCP credentials. See [setup](docs/setup.md) for running the app locally or
against real Dataproc.

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
