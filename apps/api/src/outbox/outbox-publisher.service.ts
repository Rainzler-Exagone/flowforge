import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { isNull, eq } from 'drizzle-orm';
import { db } from '../db';
import { outboxEvents } from '../db/schema';
import { KafkaService } from 'libs/kafka/src';
import { PinoLogger } from 'pino-nestjs';

@Injectable()
export class OutboxPublisherService
implements OnModuleInit, OnModuleDestroy {

  private interval?: NodeJS.Timeout;

  constructor(
    private readonly kafka: KafkaService,
    private readonly logger: PinoLogger,
  ) { }

  async onModuleInit() {
    this.logger.info('Outbox Publisher started');

    this.interval = setInterval(() => {
      void this.publishPendingEvents();
    }, 1000);

    await this.publishPendingEvents();
  }

  async onModuleDestroy() {
    if (this.interval) {
      clearInterval(this.interval);
    }
  }

  private async publishPendingEvents() {
    const events = await db
      .select()
      .from(outboxEvents)
      .where(isNull(outboxEvents.publishedAt))
      .limit(10);

    for (const event of events) {
      try {
        await this.kafka.publish(
          event.topic,
          event.payload,
          event.key,
        );

        await db
          .update(outboxEvents)
          .set({
            publishedAt: new Date(),
          })
          .where(
            eq(outboxEvents.id, event.id),
          );

        this.logger.info(
          { eventId: event.id, topic: event.topic },
          'Outbox event published',
        );
      } catch (error) {
        this.logger.error(
          {
            err: error,
            eventId: event.id,
            topic: event.topic,
          },
          'Failed to publish outbox event',
        );
      }
    }
  }
}