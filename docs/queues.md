# Background job queues (BullMQ)

Webhook delivery, email sending and exports run on Redis-backed [BullMQ](https://docs.bullmq.io/)
queues instead of inline in the request/cron path (#661).

```
producer (request / cron / outbox)
        │  queue.enqueue(...)
        ▼
   Redis  ──►  Worker (concurrency, attempts, exponential back-off)
                 ├─ webhooks : POST one event to one endpoint
                 ├─ emails   : send one rendered email over SMTP
                 └─ exports  : build a CSV/PDF and cache it in Redis
```

## Queues and job data

| Queue | Job | Payload | Work |
|-------|-----|---------|------|
| `webhooks` | `webhook.deliver` | `{ endpointId, eventType, payload }` | Signs and POSTs one event to one endpoint. |
| `emails` | `email.send` | `{ from, to, subject, text, html?, headers? }` | Sends an already-rendered email. |
| `exports` | `export.generate` | `{ stellarAddress, isAdmin, format }` | Runs the export query and caches the CSV/PDF. |

Job data is self-contained and JSON-serialisable, so a job that is still queued
(or is retried) after a restart has everything it needs. Jobs live in Redis, so
**queued work survives a process restart**; Redis persistence (`appendonly`) is
recommended in production for stronger durability guarantees.

## Configuration

Every queue has its own concurrency and retry policy:

| Variable | Default | Meaning |
|----------|---------|---------|
| `QUEUE_PREFIX` | `chainsettle` | Redis key prefix shared by all queues. |
| `QUEUE_METRICS_INTERVAL_MS` | `15000` | How often queue depths are pushed to Prometheus. |
| `QUEUE_WEBHOOKS_CONCURRENCY` | `5` | Parallel webhook deliveries per pod. |
| `QUEUE_WEBHOOKS_ATTEMPTS` | `5` | Total tries before a webhook job is parked as failed. |
| `QUEUE_WEBHOOKS_BACKOFF_MS` | `5000` | Exponential back-off base delay. |
| `QUEUE_EMAILS_CONCURRENCY` | `3` | Parallel email sends per pod. |
| `QUEUE_EMAILS_ATTEMPTS` | `5` | Total tries before an email job is parked as failed. |
| `QUEUE_EMAILS_BACKOFF_MS` | `10000` | Exponential back-off base delay. |
| `QUEUE_EXPORTS_CONCURRENCY` | `2` | Parallel export builds per pod. |
| `QUEUE_EXPORTS_ATTEMPTS` | `3` | Total tries before an export job is parked as failed. |
| `QUEUE_EXPORTS_BACKOFF_MS` | `5000` | Exponential back-off base delay. |

BullMQ's `exponential` strategy waits `backoffMs`, then `2 × backoffMs`,
`4 × backoffMs`, … between attempts. Completed jobs are removed after an hour
(or kept as the last 1 000); failed jobs are retained for 7 days for inspection.

The queue runtime connects to `REDIS_URL`. It is skipped when `SDK_GENERATE=1`
(the OpenAPI/SDK generation boot must not need Redis). When a queue cannot be
reached at enqueue time:

- webhook dispatch falls back to in-process delivery,
- `NotificationsService.sendEmail` falls back to sending inline,
- queued exports return `503` (no silent synchronous fallback — a job id is
  part of the contract).

## Metrics

Exported on `/metrics`:

| Metric | Labels | Meaning |
|--------|--------|---------|
| `chainsettle_queue_depth` | `queue`, `state` (`waiting`/`active`/`delayed`/`failed`) | Current jobs per queue state. A rising `waiting` series means workers are down or falling behind. |
| `chainsettle_queue_jobs_total` | `queue`, `job`, `result` (`enqueued`/`completed`/`retried`/`failed`) | Job lifecycle counts. `retried` is recorded per failed attempt that will be retried; `failed` only on the final attempt. |

Webhook delivery outcomes continue to be reported on
`chainsettle_webhook_deliveries_total` and
`chainsettle_webhook_delivery_duration_seconds`.

## Admin API

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/admin/queues` | Depth per state, configured concurrency, retry policy and whether a worker is registered, for every queue. |
| `POST` | `/api/v1/admin/queues/:name/pause` | Stop handing out jobs (in-flight jobs finish; queued jobs wait). |
| `POST` | `/api/v1/admin/queues/:name/resume` | Resume a paused queue. |

All three require an `ADMIN` JWT. `:name` is one of `webhooks`, `emails`,
`exports`.

## Queued exports

The synchronous endpoints (`GET /shipments/export`, `GET /shipments/:id/export`)
remain for backwards compatibility. For large exports use the job endpoints:

1. `POST /api/v1/shipments/export-jobs` body `{ "format": "csv" | "pdf" }`
   → `202 { jobId, status: "queued", statusUrl }`.
2. Poll `GET /api/v1/shipments/export-jobs/:id` →
   `{ status: "queued"|"processing"|"completed"|"failed", result?, error?, downloadUrl? }`.
3. `GET /api/v1/shipments/export-jobs/:id/download` → the file.

The finished artifact is cached in Redis for one hour; jobs are only visible to
the user who requested them (job data is scoped by Stellar address; admins'
exports are admin-scoped).

## Operational notes

- Queues are safe with multiple pods: BullMQ workers atomically claim jobs, so
  each job is processed once even with several replicas.
- Pause a queue before draining a pod for deploy; jobs remain in Redis and are
  picked up when the queue resumes.
- Webhook HTTP failures keep their existing database-level retry policy
  (`nextRetryAt` + the `webhook-retry-queue` cron) in addition to queue-level
  retries for infrastructure errors.
