import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { db } from '../db';
import { jobs } from '../db/schema';
import { KafkaService } from 'libs/kafka/src/kafka.service';
import { eq } from 'drizzle-orm';

@Injectable()
export class JobsService {

  constructor(
    private readonly kafka: KafkaService,
  ) { }

  async create(
    type: string,
    input?: unknown,
    parameters?: unknown,
  ) {
    const [job] = await db
      .insert(jobs)
      .values({
        type,
        input,
        parameters,
      })
      .returning();

    await this.kafka.publish(
      'flowforge.jobs',
      {
        jobId: job.id,
      },
      String(job.id),
    );

    return job;
  }



  async retryFailedJob(jobId: string) {
    const [job] = await db
      .select()
      .from(jobs)
      .where(eq(jobs.id, jobId))
      .limit(1);

    if (!job) {
      throw new NotFoundException('Job not found');
    }

    if (job.status !== 'failed') {
      throw new BadRequestException(
        'Only failed jobs can be retried',
      );
    }

    await db
      .update(jobs)
      .set({
        status: 'queued',
        attempts: 0,
      })
      .where(eq(jobs.id, job.id));

    await this.kafka.publish(
      'flowforge.jobs',
      {
        jobId: job.id,
      },
      String(job.id),
    );

    return {
      message: 'Job queued for retry',
      jobId: job.id,
    };
  }
  async findAll() {
    return db.select().from(jobs);
  }
}