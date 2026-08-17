import { Injectable, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { eq } from 'drizzle-orm';
import { db } from './db';
import { jobs } from './db/schema';

@Injectable()
export class ImageWorkerService
  implements OnModuleInit, OnModuleDestroy {
  private isRunning = true;

  async onModuleInit() {
    console.log('Image Worker started');

    await this.workerLoop();
  }

  async onModuleDestroy() {
    console.log('Stopping Image Worker...');
    this.isRunning = false;
  }

  private async workerLoop() {
    while (this.isRunning) {
      try {
        await this.processJobs();
      } catch (error) {
        console.error('Worker loop error:', error);
      }

      await this.sleep(2000);
    }
  }

  private async processJobs() {
    const job = await db.transaction(async (tx) => {
      const [job] = await tx
        .select()
        .from(jobs)
        .where(eq(jobs.status, 'queued'))
        .for('update', { skipLocked: true })
        .limit(1);

      if (!job) {
        return null;
      }

      await tx
        .update(jobs)
        .set({
          status: 'running',
        })
        .where(eq(jobs.id, job.id));

      return job;
    });

    if (!job) {
      return;
    }

    await this.processJob(job);
  }

  private async processJob(job: typeof jobs.$inferSelect) {
    console.log(`Processing job ${job.id}: ${job.type}`);


    try {
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
        })
        .where(eq(jobs.id, job.id));

      console.log(`Job ${job.id} completed`);
    } catch (error) {
      await db
        .update(jobs)
        .set({
          status: 'failed',
        })
        .where(eq(jobs.id, job.id));

      console.error(`Job ${job.id} failed`, error);
    }
  }

  private async resizeImage(
    job: typeof jobs.$inferSelect,
  ) {
    console.log('Resize parameters:', job.parameters);

    await this.sleep(2000);
  }

  private sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}