import { integer } from 'drizzle-orm/pg-core';
import {
  jsonb,
  pgEnum,
  pgTable,
  text,
  timestamp,
  uuid,
} from 'drizzle-orm/pg-core';

export const jobStatusEnum = pgEnum('job_status', [
  'queued',
  'running',
  'completed',
  'failed',
]);

export const jobs = pgTable('jobs', {
  id: uuid('id').defaultRandom().primaryKey(),

  type: text('type').notNull(),

  status: jobStatusEnum('status')
    .default('queued')
    .notNull(),

  input: jsonb('input'),

  parameters: jsonb('parameters'),

  result: jsonb('result'),

  attempts: integer('attempts').notNull().default(0),

  maxAttempts: integer('max_attempts').notNull().default(3),


  lockedAt: timestamp('locked_at'),
  
  createdAt: timestamp('created_at')
    .defaultNow()
    .notNull(),
});