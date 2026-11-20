# Data Model — MongoDB Collections

The service stores five collections in MongoDB. Field names are camelCase and enums are
stored as strings. `Instant` values are stored as BSON dates.

## `jobs` (one document per logical job)

| Field | Type | Notes |
|---|---|---|
| `_id` | ObjectId | Mongo document id |
| `jobId` | string | Business key — **unique index** |
| `eventId` | string | Kafka dedup key — **unique index** |
| `correlationId` | string | End-to-end trace id |
| `jobType` | string | e.g. `SEGMENT_PREVIEW` — indexed |
| `priority` | `HIGH` \| `NORMAL` \| `LOW` | Dispatch priority |
| `status` | `JobStatus` enum | Logical lifecycle state — indexed |
| `attemptCount` | int | Execution attempts so far |
| `maxAttempts` | int | Resolved retry limit |
| `nextRetryAt` | date | When a RETRY_WAIT job may be re-queued |
| `lastErrorType` | `FailureType` enum | TRANSIENT / PERMANENT / TIMEOUT / AMBIGUOUS |
| `lastError` | string | Error message |
| `dataprocJobId` | string | Dataproc execution identifier |
| `dataprocProjectId` | string | GCP project |
| `dataprocRegion` | string | GCP region |
| `jobParameters` | string | Serialized business parameters (JSON) passed to the Spark job as an argument |
| `executionTimeoutSeconds` | int | Max execution duration |
| `leaseOwner` | string | Replica id holding the dispatch reservation |
| `leaseExpiresAt` | date | Reservation expiry (reconciliation) |
| `receivedAt` / `queuedAt` / `dispatchedAt` / `submittedAt` / `startedAt` / `completedAt` | date | Lifecycle timestamps |
| `createdAt` / `updatedAt` | date | Audit timestamps (auto-managed) |

Compound indexes: `{status, priority, createdAt}` and `{jobType, status}`.

Sample document:

```json
{
  "jobId": "job-1001",
  "eventId": "evt-1001",
  "correlationId": "segment-0001",
  "jobType": "SEGMENT_PREVIEW",
  "priority": "HIGH",
  "status": "SUCCEEDED",
  "attemptCount": 2,
  "maxAttempts": 3,
  "dataprocJobId": "noop-job-1001-2",
  "jobParameters": "{\"previewMessage\":\"...\",\"segmentCriteria\":{...}}",
  "executionTimeoutSeconds": 1800,
  "receivedAt": "2026-11-19T12:00:00Z",
  "completedAt": "2026-11-19T12:05:00Z"
}
```

## `job_execution_attempts` (one document per physical attempt)

| Field | Type | Notes |
|---|---|---|
| `_id` | ObjectId | |
| `jobId` | string | Parent job id — indexed |
| `attemptNumber` | int | 1-based — unique together with `jobId` |
| `status` | `JobStatus` enum | SUBMITTED / RUNNING / SUCCEEDED / FAILED / … |
| `dataprocJobId` | string | Dataproc id for this attempt |
| `errorType` | `FailureType` enum | Failure classification |
| `errorMessage` | string | Failure detail |
| `submittedAt` / `startedAt` / `completedAt` | date | Attempt timestamps |
| `createdAt` | date | Auto-managed |

## `job_status_history` (append-only audit trail)

| Field | Type | Notes |
|---|---|---|
| `_id` | ObjectId | |
| `jobId` | string | — indexed |
| `fromStatus` | `JobStatus` enum | nullable (first transition) |
| `toStatus` | `JobStatus` enum | New state |
| `reason` | string | Human-readable reason |
| `correlationId` | string | Trace id |
| `occurredAt` | date | When the transition happened |

## `outbox_events` (transactional outbox)

| Field | Type | Notes |
|---|---|---|
| `_id` | ObjectId | |
| `eventId` | string | Consumer dedup key — **unique index** |
| `jobId` | string | Source job |
| `eventType` | `JOB_COMPLETED` \| `JOB_FAILED` | |
| `topic` | string | Kafka destination |
| `payload` | string | Serialized event JSON |
| `correlationId` | string | Trace id |
| `status` | `PENDING` \| `PUBLISHED` \| `FAILED` | — indexed |
| `createdAt` / `publishedAt` | date | |

## `scheduler_state` (singleton, `_id = "singleton"`)

| Field | Type | Notes |
|---|---|---|
| `_id` | string | Fixed value `"singleton"` |
| `paused` | boolean | Dispatcher pause flag (shared across replicas) |
| `pausedAt` / `resumedAt` | date | Last pause/resume timestamps |
| `updatedAt` | date | Auto-managed |
