import { Injectable } from '@nestjs/common';
import { db } from '../db';
import { jobs } from '../db/schema';

@Injectable()
export class JobsService {

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

  return job;
}

  async findAll() {
    return db.select().from(jobs);
  }
}