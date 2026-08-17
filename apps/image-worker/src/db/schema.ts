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
  id: uuid('id').primaryKey(),
  type: text('type').notNull(),
  status: jobStatusEnum('status').notNull(),
  input: jsonb('input'),
  parameters: jsonb('parameters'),
  result: jsonb('result'),
  createdAt: timestamp('created_at').notNull(),
});