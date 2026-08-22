import { Injectable, OnModuleInit } from '@nestjs/common';
import { db } from 'apps/image-worker/src/db';
import { sql } from 'drizzle-orm';
import { PinoLogger } from 'pino-nestjs';

@Injectable()
export class DatabaseService implements OnModuleInit {
  constructor(private readonly logger: PinoLogger) {}

  async onModuleInit() {
    try {
      await db.execute(sql`SELECT 1`);

      this.logger.info('Database connected');
    } catch (error) {
      this.logger.error(
        { err: error },
        'Database connection failed',
      );

      throw error;
    }
  }
}