import { Injectable } from '@nestjs/common';
import { db } from '../db';
import { jobs } from '../db/schema';
import { KafkaService } from 'libs/kafka/src/kafka.service';

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

  async findAll() {
    return db.select().from(jobs);
  }
}