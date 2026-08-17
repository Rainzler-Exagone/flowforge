import { Injectable, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { eq } from 'drizzle-orm';
import { db } from './db';
import { jobs } from './db/schema';

@Injectable()
export class ImageWorkerService
  implements OnModuleInit, OnModuleDestroy {

  async onModuleInit() {
    console.log('Image Worker started');
  }

  async onModuleDestroy() {
    console.log('Stopping Image Worker...');
  }



  public async processJob(job: typeof jobs.$inferSelect) {
    console.log(`Processing job ${job.id}: ${job.type}`);


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