# FlowForge

**FlowForge** is a production-oriented distributed job processing system built with **NestJS, Kafka, PostgreSQL, Drizzle ORM, and Docker**.

It demonstrates how to build a reliable asynchronous processing pipeline with multiple workers, durable job state, retries, dead-letter queues, worker leases, stale-job recovery, structured logging, and containerized services.

## Architecture

```text
                    ┌──────────────┐
                    │    Client    │
                    └──────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
                    │   API        │
                    │   NestJS     │
                    └──────┬───────┘
                           │
                    PostgreSQL
                           │
                           ▼
                    ┌──────────────┐
                    │    Outbox    │
                    │    Events    │
                    └──────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
                    │    Kafka     │
                    │ flowforge.jobs
                    └──────┬───────┘
                           │
                 ┌─────────┴─────────┐
                 ▼                   ▼
          ┌─────────────┐     ┌─────────────┐
          │Image Worker │     │Image Worker │
          │   Worker 1  │     │   Worker 2  │
          └──────┬──────┘     └──────┬──────┘
                 │                   │
                 └─────────┬─────────┘
                           ▼
                    ┌──────────────┐
                    │ PostgreSQL   │
                    │ Job Results  │
                    └──────────────┘
```

## Key Features

* **Kafka-based asynchronous job processing**
* **Multiple worker support**
* **Database-backed job state**
* **Transactional outbox pattern**
* **Job retries with exponential backoff**
* **Dead-letter queue (DLQ)**
* **Worker leases**
* **Heartbeat-based lease renewal**
* **Automatic stale-job recovery**
* **Idempotent job result creation**
* **Structured logging with Pino**
* **Environment-based log levels**
* **Dockerized API and workers**
* **TypeScript + NestJS monorepo**
* **PostgreSQL with Drizzle ORM**
* **Automated linting and tests**
* **CI/CD-ready container builds**

## Technology Stack

| Component       | Technology   |
| --------------- | ------------ |
| Runtime         | Node.js 24   |
| Framework       | NestJS       |
| Language        | TypeScript   |
| Database        | PostgreSQL   |
| ORM             | Drizzle ORM  |
| Messaging       | Apache Kafka |
| Kafka Client    | KafkaJS      |
| Logging         | Pino         |
| Containers      | Docker       |
| Package Manager | npm          |

## Project Structure

```text
flowforge-api/
├── apps/
│   ├── api/
│   │   └── src/
│   │       ├── jobs/
│   │       ├── outbox/
│   │       ├── db/
│   │       ├── api.module.ts
│   │       └── main.ts
│   │
│   ├── image-worker/
│   │   └── src/
│   │       ├── db/
│   │       ├── image-worker.service.ts
│   │       └── main.ts
│   │
│   └── scheduler/
│       └── src/
│
├── libs/
│   └── kafka/
│       └── src/
│           ├── kafka.module.ts
│           └── kafka.service.ts
│
├── dockerfile
├── dockerfile.worker
├── docker-compose.yml
├── nest-cli.json
├── package.json
└── README.md
```

## Job Processing Flow

A job follows this general lifecycle:

```text
queued
  │
  ▼
running
  │
  ├──────────────► completed
  │
  ├── failure ───► queued
  │                  │
  │                  └── retry
  │
  └── max retries ─► failed
                         │
                         ▼
                        DLQ
```

### 1. Job creation

The API creates a persistent job record in PostgreSQL.

### 2. Outbox publishing

The job event is stored in the database outbox. The outbox publisher then publishes the event to Kafka.

This prevents the system from successfully committing a database transaction while failing to publish the corresponding Kafka event.

### 3. Kafka distribution

Workers consume jobs from the `flowforge.jobs` topic.

Kafka consumer groups allow multiple worker instances to process jobs concurrently.

### 4. Job claiming

Before processing, a worker atomically claims the job:

```text
status = queued
        ↓
status = running
lockedAt = now()
leaseId = unique worker lease
```

The lease prevents another worker from completing a job that has already been claimed.

### 5. Processing

The worker executes the appropriate job handler.

Currently, the example workload includes:

```text
resize_image
```

### 6. Completion

After successful processing, the worker verifies that it still owns the lease before marking the job as completed.

This prevents a worker with an expired lease from overwriting the state of a job that has already been reclaimed.

## Retry Strategy

Failed jobs are retried using exponential backoff.

The delay increases with each attempt:

```text
Attempt 1 → 1s
Attempt 2 → 2s
Attempt 3 → 4s
...
```

Once the maximum number of attempts is reached, the job is marked as failed and published to the dead-letter queue:

```text
flowforge.jobs.dlq
```

## Worker Lease & Recovery

Workers maintain a lease while processing a job.

The worker periodically renews:

```text
lockedAt
```

If a worker crashes or becomes unavailable, its lease eventually expires.

The recovery process detects stale jobs and either:

* requeues them for another attempt, or
* moves them to the DLQ when the retry limit is reached.

This provides recovery from worker crashes without requiring manual intervention.

## Idempotency

Job results use a unique job association so that processing the same job more than once does not create duplicate results.

For example:

```text
Job
 │
 ├── Worker A creates result
 │
 └── Worker B retries same job
          │
          └── duplicate insert ignored
```

This is particularly important in distributed systems because message delivery and job execution are not inherently exactly-once.

## Logging

FlowForge uses **Pino** for structured application logging.

Development logging uses `pino-pretty`, while production uses structured JSON logs.

Log levels can be configured through:

```env
LOG_LEVEL=info
```

Example structured log:

```json
{
  "level": 30,
  "jobId": "8c9...",
  "attempt": 2,
  "msg": "Processing job"
}
```

This makes logs suitable for aggregation and observability platforms.

## Environment Variables

Example configuration:

```env
NODE_ENV=development
PORT=3000

DATABASE_URL=postgresql://postgres:postgres@localhost:5432/flowforge

KAFKA_BROKER=localhost:9092

LOG_LEVEL=info
```

Do not commit real credentials or production secrets to the repository.

## Running Locally

### Install dependencies

```bash
npm ci
```

### Start infrastructure

```bash
docker compose up -d
```

### Run database migrations

```bash
npm run db:migrate
```

### Start the API

```bash
npm run start:dev
```

### Start the image worker

```bash
npm run start:dev image-worker
```

## Docker

FlowForge uses separate container images for the API and image worker.

### API image

```bash
docker build -t flowforge-api:local -f dockerfile .
```

### Worker image

```bash
docker build -t flowforge-worker:local -f dockerfile.worker .
```

The worker has its own Dockerfile because it is a separate application inside the NestJS monorepo and has a different runtime entry point:

```text
API
→ dist/apps/api/main.js

Image Worker
→ dist/apps/image-worker/main.js
```

Both Dockerfiles use multi-stage builds so development dependencies and the build environment are excluded from the final production image.

## Testing

Run the test suite with:

```bash
npm test
```

Run tests in watch mode:

```bash
npm run test:watch
```

Generate coverage:

```bash
npm run test:cov
```

## Linting

Run ESLint with:

```bash
npm run lint
```

CI is configured to fail when linting or tests fail. This prevents broken code from progressing through the deployment pipeline.

## CI/CD

The project is designed around a CI/CD workflow:

```text
Git Push
   │
   ▼
GitHub Actions
   │
   ├── Install dependencies
   ├── Type-check
   ├── Lint
   ├── Test
   ├── Build
   │
   ▼
Build Docker Images
   │
   ▼
Push Images to Container Registry
   │
   ▼
Deploy
```

The goal is to ensure that only code passing the project's quality gates reaches the deployment stage.

## Reliability Considerations

FlowForge focuses on several failure scenarios commonly encountered in distributed systems:

### Worker crash

A worker can terminate while processing a job.

**Recovery:** the lease eventually expires and the job is requeued.

### Duplicate delivery

A job can be delivered or processed more than once.

**Protection:** job state checks and idempotent result creation.

### Maximum retries exceeded

A job repeatedly fails.

**Recovery:** the job is moved to the DLQ.

### Database/Kafka consistency

A database transaction may succeed while publishing an event fails.

**Protection:** transactional outbox pattern.

### Multiple workers

Several workers may receive the same job concurrently.

**Protection:** atomic job claiming and worker leases.

## Development Goals

This project is primarily a demonstration of production-oriented backend architecture and distributed job processing.

Future improvements may include:

* Prometheus metrics
* OpenTelemetry tracing
* Grafana dashboards
* Kafka consumer lag monitoring
* Kubernetes deployment
* Horizontal worker autoscaling
* Object storage for processed images
* More job types
* Integration and end-to-end testing
* Automated production deployment

## License

This project is licensed under the MIT License.
