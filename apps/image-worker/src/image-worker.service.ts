import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { and, eq, lt } from 'drizzle-orm';
import { db } from './db';
import { jobResults, jobs } from './db/schema';
import { KafkaService } from 'libs/kafka/src';
import { randomUUID } from 'crypto';
import { PinoLogger } from 'pino-nestjs';

@Injectable()
export class ImageWorkerService
  implements OnModuleInit, OnModuleDestroy {

  constructor(
    private readonly kafka: KafkaService,
    private readonly logger: PinoLogger,
  ) {}

  private readonly JOB_LEASE_MS = 5 * 60 * 1000;
  private recoveryInterval?: NodeJS.Timeout;

  async onModuleInit() {
    this.logger.info('Image Worker started');

    this.recoveryInterval = setInterval(() => {
      void this.recoverStaleJobs();
    }, 60_000);
  }

  async onModuleDestroy() {
    this.logger.info('Stopping Image Worker...');

    if (this.recoveryInterval) {
      clearInterval(this.recoveryInterval);
    }
  }

  public async processJob(job: typeof jobs.$inferSelect) {
    const leaseId = randomUUID();

    const [currentJob] = await db
      .select()
      .from(jobs)
      .where(eq(jobs.id, job.id))
      .limit(1);

    if (!currentJob) {
      this.logger.info(
        { jobId: job.id },
        'Job no longer exists. Skipping',
      );
      return;
    }

    if (currentJob.status === 'completed') {
      this.logger.info(
        { jobId: job.id },
        'Job already completed. Skipping',
      );
      return;
    }

    let heartbeatInterval: NodeJS.Timeout | undefined;

    try {
      const claimedJob = await db
        .update(jobs)
        .set({
          status: 'running',
          lockedAt: new Date(),
          leaseId,
        })
        .where(
          and(
            eq(jobs.id, job.id),
            eq(jobs.status, 'queued'),
          ),
        )
        .returning();

      if (claimedJob.length === 0) {
        this.logger.info(
          { jobId: job.id },
          'Job was already claimed. Skipping',
        );

        return;
      }

      this.logger.info(
        { jobId: job.id },
        'Job claimed by this worker',
      );

      heartbeatInterval = setInterval(() => {
        void this.renewLease(job.id, leaseId);
      }, 60_000);

      this.logger.info(
        {
          jobId: job.id,
          jobType: job.type,
          attempt: currentJob.attempts + 1,
        },
        'Processing job',
      );

      switch (job.type) {
        case 'resize_image':
          await this.resizeImage(job);
          break;

        default:
          throw new Error(`Unsupported job type: ${job.type}`);
      }

      const completedJob = await db
        .update(jobs)
        .set({
          status: 'completed',
          attempts: job.attempts + 1,
          lockedAt: null,
          leaseId: null,
        })
        .where(
          and(
            eq(jobs.id, job.id),
            eq(jobs.status, 'running'),
            eq(jobs.leaseId, leaseId),
          ),
        )
        .returning();

      if (completedJob.length === 0) {
        this.logger.warn(
          { jobId: job.id },
          'Job lease expired or was reclaimed. This worker will not complete it.',
        );

        return;
      }

      this.logger.info(
        { jobId: job.id },
        'Job completed',
      );
    } catch (error) {
      this.logger.error(
        {
          err: error,
          jobId: job.id,
        },
        'Job processing failed',
      );

      await this.retryJob(currentJob, leaseId);
    } finally {
      if (heartbeatInterval) {
        clearInterval(heartbeatInterval);
      }
    }
  }

  private async resizeImage(
    job: typeof jobs.$inferSelect,
  ) {
    this.logger.info(
      {
        jobId: job.id,
        parameters: job.parameters,
      },
      'Resize parameters',
    );

    await this.sleep(2000);

    const result = {
      output: `jobs/${job.id}/resized.jpg`,
      width: (job.parameters as any).width,
      height: (job.parameters as any).height,
    };

    const [createdResult] = await db
      .insert(jobResults)
      .values({
        jobId: job.id,
        result,
      })
      .onConflictDoNothing({
        target: jobResults.jobId,
      })
      .returning();

    if (!createdResult) {
      this.logger.info(
        { jobId: job.id },
        'Result already exists. Skipping duplicate result',
      );

      return;
    }

    this.logger.info(
      {
        jobId: job.id,
        result,
      },
      'Created job result',
    );

    // Intentional crash used for the idempotency/recovery test.
    process.exit(1);
  }

  private sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private async retryJob(
    job: typeof jobs.$inferSelect,
    leaseId: string,
  ) {
    const nextAttempt = job.attempts + 1;

    if (nextAttempt >= job.maxAttempts) {
      await db
        .update(jobs)
        .set({
          status: 'failed',
          attempts: nextAttempt,
        })
        .where(
          and(
            eq(jobs.id, job.id),
            eq(jobs.status, 'running'),
            eq(jobs.leaseId, leaseId),
          ),
        );

      await this.kafka.publish(
        'flowforge.jobs.dlq',
        {
          jobId: job.id,
          type: job.type,
          input: job.input,
          parameters: job.parameters,
          attempts: nextAttempt,
          reason: 'max_retries_exceeded',
        },
        String(job.id),
      );

      this.logger.warn(
        {
          jobId: job.id,
          attempts: nextAttempt,
        },
        'Job moved to DLQ after maximum retries',
      );

      return;
    }

    const result = await db
      .update(jobs)
      .set({
        status: 'queued',
        attempts: nextAttempt,
        lockedAt: null,
        leaseId: null,
      })
      .where(
        and(
          eq(jobs.id, job.id),
          eq(jobs.status, 'running'),
          eq(jobs.leaseId, leaseId),
        ),
      )
      .returning();

    if (result.length === 0) {
      this.logger.info(
        { jobId: job.id },
        'Job is no longer owned by this worker. Retry cancelled',
      );

      return;
    }

    const delay = Math.pow(2, nextAttempt - 1) * 1000;

    this.logger.warn(
      {
        jobId: job.id,
        attempt: nextAttempt,
        delay,
      },
      'Job failed. Retrying',
    );

    await this.sleep(delay);

    await this.kafka.publish(
      'flowforge.jobs',
      {
        jobId: job.id,
      },
      String(job.id),
    );
  }

  private async recoverStaleJobs() {
    const cutoff = new Date(
      Date.now() - this.JOB_LEASE_MS,
    );

    const staleJobs = await db
      .select()
      .from(jobs)
      .where(
        and(
          eq(jobs.status, 'running'),
          lt(jobs.lockedAt, cutoff),
        ),
      );

    for (const job of staleJobs) {
      const nextAttempt = job.attempts + 1;

      this.logger.warn(
        {
          jobId: job.id,
          attempt: nextAttempt,
        },
        'Recovering stale job',
      );

      if (nextAttempt >= job.maxAttempts) {
        await db
          .update(jobs)
          .set({
            status: 'failed',
            attempts: nextAttempt,
            lockedAt: null,
            leaseId: null,
          })
          .where(
            and(
              eq(jobs.id, job.id),
              eq(jobs.status, 'running'),
              lt(jobs.lockedAt, cutoff),
            ),
          );

        await this.kafka.publish(
          'flowforge.jobs.dlq',
          {
            jobId: job.id,
            type: job.type,
            input: job.input,
            parameters: job.parameters,
            attempts: nextAttempt,
            reason: 'worker_lease_expired',
          },
          String(job.id),
        );

        this.logger.warn(
          {
            jobId: job.id,
            attempts: nextAttempt,
          },
          'Job moved to DLQ after worker lease expired',
        );

        continue;
      }

      const result = await db
        .update(jobs)
        .set({
          status: 'queued',
          attempts: nextAttempt,
          lockedAt: null,
          leaseId: null,
        })
        .where(
          and(
            eq(jobs.id, job.id),
            eq(jobs.status, 'running'),
            lt(jobs.lockedAt, cutoff),
          ),
        )
        .returning();

      // Another worker may have changed the job
      // between SELECT and UPDATE.
      if (result.length === 0) {
        continue;
      }

      await this.kafka.publish(
        'flowforge.jobs',
        {
          jobId: job.id,
        },
        String(job.id),
      );

      this.logger.info(
        { jobId: job.id },
        'Stale job recovered and requeued',
      );
    }
  }

  private async renewLease(
    jobId: string,
    leaseId: string,
  ) {
    const result = await db
      .update(jobs)
      .set({
        lockedAt: new Date(),
      })
      .where(
        and(
          eq(jobs.id, jobId),
          eq(jobs.status, 'running'),
          eq(jobs.leaseId, leaseId),
        ),
      )
      .returning({
        id: jobs.id,
      });

    if (result.length === 0) {
      this.logger.warn(
        { jobId },
        'Job lease is no longer owned by this worker',
      );

      return false;
    }

    this.logger.debug(
      { jobId },
      'Lease renewed',
    );

    return true;
  }
}