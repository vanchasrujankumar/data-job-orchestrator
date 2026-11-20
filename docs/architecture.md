# Architecture

## End-to-end flow

```mermaid
flowchart LR
    subgraph Upstream
        P[Producer<br/>job-requests topic]
    end

    subgraph Service
        C[Kafka Consumer<br/>validate + dedup] --> S[JobService<br/>persist RECEIVED → QUEUED]
        S --> DB[(MongoDB<br/>jobs / attempts / history / outbox)]
        D[Dispatcher<br/>claim + submit] --> DB
        D --> G[DataprocClient<br/>Serverless or Noop]
        M[Monitor<br/>poll status] --> G
        M --> DB
        R[RetryScheduler] --> DB
        RC[Reconciliation] --> DB
        O[Outbox Publisher] --> DB
    end

    P -->|JSON event| C
    O -->|completion/failure| K[Kafka<br/>job-events topic]

    style C fill:#1f77b4,color:#fff
    style S fill:#2ca02c,color:#fff
    style D fill:#ff7f0e,color:#fff
    style M fill:#9467bd,color:#fff
    style R fill:#d62728,color:#fff
    style O fill:#17becf,color:#fff
    style DB fill:#7f7f7f,color:#fff
```

1. A producer writes a JSON request to `job-requests`. The consumer validates, deduplicates on
   `eventId`, persists (RECEIVED → QUEUED) and only then acknowledges the offset.
2. The **Dispatcher** claims queued jobs atomically (MongoDB `findAndModify`) when a concurrency
   slot is free, and submits them to Dataproc asynchronously.
3. The **Monitor** polls Dataproc, maps states, records outcomes and releases slots.
4. The **RetryScheduler** re-queues failed jobs after exponential backoff; the **Outbox
   publisher** relays terminal events to `job-events`; **Reconciliation** recovers interrupted
   dispatches.

## Job lifecycle state machine

```mermaid
stateDiagram-v2
    direction LR

    [*] --> RECEIVED
    RECEIVED --> QUEUED
    RECEIVED --> CANCELLED
    RECEIVED --> FAILED

    QUEUED --> DISPATCHING
    QUEUED --> CANCELLED

    DISPATCHING --> SUBMITTED
    DISPATCHING --> RETRY_WAIT
    DISPATCHING --> FAILED
    DISPATCHING --> CANCEL_REQUESTED
    DISPATCHING --> QUEUED

    SUBMITTED --> RUNNING
    SUBMITTED --> SUCCEEDED
    SUBMITTED --> FAILED
    SUBMITTED --> RETRY_WAIT
    SUBMITTED --> TIMED_OUT
    SUBMITTED --> CANCEL_REQUESTED

    RUNNING --> SUCCEEDED
    RUNNING --> FAILED
    RUNNING --> RETRY_WAIT
    RUNNING --> TIMED_OUT
    RUNNING --> CANCEL_REQUESTED

    RETRY_WAIT --> QUEUED
    RETRY_WAIT --> FAILED
    RETRY_WAIT --> CANCELLED

    CANCEL_REQUESTED --> CANCELLED
    CANCEL_REQUESTED --> TIMED_OUT
    CANCEL_REQUESTED --> RETRY_WAIT

    FAILED --> QUEUED
    TIMED_OUT --> QUEUED

    SUCCEEDED --> [*]
    FAILED --> [*]
    CANCELLED --> [*]
    TIMED_OUT --> [*]

    classDef slot fill:#ff7f0e,color:#fff
    classDef terminal fill:#2ca02c,color:#fff
    classDef retry fill:#d62728,color:#fff
    classDef init fill:#1f77b4,color:#fff

    class DISPATCHING,SUBMITTED,RUNNING,CANCEL_REQUESTED slot
    class SUCCEEDED,FAILED,CANCELLED,TIMED_OUT terminal
    class RETRY_WAIT retry
    class RECEIVED,QUEUED init
```

**Legend:** orange = holds a concurrency slot · green = terminal · red = retry wait · blue = queued/received.

The `JobStateMachine` is the single source of truth for valid transitions; any write path that
changes a job's status must go through it.

## Domain model (UML)

```mermaid
classDiagram
    direction LR

    class JobEntity {
        +String id
        +String jobId
        +String eventId
        +String correlationId
        +String jobType
        +JobPriority priority
        +JobStatus status
        +int attemptCount
        +int maxAttempts
        +Instant nextRetryAt
        +String dataprocJobId
        +String jobParameters
        +String leaseOwner
    }

    class ExecutionAttemptEntity {
        +String id
        +String jobId
        +int attemptNumber
        +JobStatus status
        +String dataprocJobId
        +FailureType errorType
    }

    class JobStatusHistoryEntity {
        +String jobId
        +JobStatus fromStatus
        +JobStatus toStatus
        +String reason
    }

    class OutboxEventEntity {
        +String eventId
        +String jobId
        +String eventType
        +String topic
        +String payload
        +OutboxStatus status
    }

    class JobService {
        +claim(id, owner, expiry)
        +markSubmitted(jobId, dataprocJobId)
        +complete(jobId)
        +failRetryable(jobId, ...)
        +failPermanent(jobId, ...)
        +retryManually(jobId)
        +requestCancel(jobId)
    }

    class JobDispatcher
    class JobMonitor
    class RetryScheduler
    class ReconciliationService
    class OutboxPublisher
    class DataprocClient

    JobEntity "1" --> "0..*" ExecutionAttemptEntity : attempts
    JobEntity "1" --> "0..*" JobStatusHistoryEntity : history
    JobEntity "1" --> "0..*" OutboxEventEntity : outbox events

    JobDispatcher --> JobService
    JobMonitor --> JobService
    RetryScheduler --> JobService
    ReconciliationService --> JobService
    JobDispatcher --> DataprocClient
    JobMonitor --> DataprocClient
    OutboxPublisher --> OutboxEventEntity
```

## Package layout

```
com.dataorchestrator
├── api/            REST controllers + DTOs
├── alert/          AlertService abstraction + logging impl
├── common/         correlation filter, exception handling, domain exceptions
├── config/         properties, Kafka, OpenTelemetry, MongoDB, OpenAPI
├── dataproc/       DataprocClient (Serverless + Noop), SparkJobSpec, SparkJobDefinition
├── job/            documents, repositories, state machine, JobService
├── metrics/        OpenTelemetry metrics
├── outbox/         transactional outbox + publisher
├── request/        Kafka consumer + validation + dedup
└── scheduler/      dispatcher, monitor, retry, reconciliation, concurrency
```

## Reliability design

| Concern | Mechanism |
|---|---|
| Duplicate events | Unique index on `eventId`; repeated deliveries never create a second job |
| Multi-replica concurrency | Atomic `findAndModify` claim; the DB is the single source of truth |
| Idempotent submission | Deterministic Dataproc request id (`jobId-attempt-N`) |
| Lost events on restart | Transactional outbox (MongoDB replica set, see `docker-compose.yml`) |
| Interrupted dispatch | Stale `DISPATCHING` leases reverted to `QUEUED` on startup and periodically |
| Ambiguous submission | Reconciliation + deterministic request id avoid duplicate executions |
| Retry safety | Retries respect concurrency limits and re-enter via the normal dispatcher |
| Metric cardinality | Only low-cardinality labels (job type); never the job id |
