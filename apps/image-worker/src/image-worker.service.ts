import { Injectable, OnModuleInit, OnModuleDestroy, NotFoundException, BadRequestException } from '@nestjs/common';
import { eq } from 'drizzle-orm';
import { db } from './db';
import { jobs } from './db/schema';
import { KafkaService } from 'libs/kafka/src';

@Injectable()
export class ImageWorkerService
  implements OnModuleInit, OnModuleDestroy {

  constructor(
    private readonly kafka: KafkaService,
  ) { }


  async onModuleInit() {
    console.log('Image Worker started');
  }

  async onModuleDestroy() {
    console.log('Stopping Image Worker...');
  }



  public async processJob(job: typeof jobs.$inferSelect) {
    console.log(
      `Processing job ${job.id}: ${job.type} (attempt ${job.attempts + 1})`,
    );


    try {

      await db
        .update(jobs)
        .set({
          status: 'running',
        })
        .where(eq(jobs.id, job.id));

      switch (job.type) {
        case 'resize_image':
          await this.resizeImage(job);
          break;

        default:
          throw new Error(`Unsupported job type: ${job.type}`);
      }

      await db
        .update(jobs)
        .set({
          status: 'completed',
          attempts: job.attempts + 1,
        })
        .where(eq(jobs.id, job.id));

      console.log(`Job ${job.id} completed`);
    } catch (error) {


      console.error(`Job ${job.id} failed`, error);
      await this.retryJob(job);

      console.error(`Job ${job.id} failed`, error);
    }
  }

  private async resizeImage(
    job: typeof jobs.$inferSelect,
  ) {
    console.log('Resize parameters:', job.parameters);

    await this.sleep(2000);

    throw new Error('Simulated image processing failure');

  }

  private sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }


  private async retryJob(
    job: typeof jobs.$inferSelect,
  ) {
    const nextAttempt = job.attempts + 1;

    if (nextAttempt >= job.maxAttempts) {
      await db
        .update(jobs)
        .set({
          status: 'failed',
          attempts: nextAttempt,
        })
        .where(eq(jobs.id, job.id));

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

      console.log(
        `Job ${job.id} moved to DLQ after ${nextAttempt} attempts`,
      );

      return;
    }

    await db
      .update(jobs)
      .set({
        status: 'queued',
        attempts: nextAttempt,
      })
      .where(eq(jobs.id, job.id));

    const delay = Math.pow(2, nextAttempt - 1) * 1000;

    console.log(
      `Job ${job.id} failed. Retrying in ${delay}ms`,
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



}