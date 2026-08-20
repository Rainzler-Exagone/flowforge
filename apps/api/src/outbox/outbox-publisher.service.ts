import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { isNull, eq } from 'drizzle-orm';
import { db } from '../db';
import { outboxEvents } from '../db/schema';
import { KafkaService } from 'libs/kafka/src';

@Injectable()
export class OutboxPublisherService
  implements OnModuleInit, OnModuleDestroy {

  private interval?: NodeJS.Timeout;

  constructor(
    private readonly kafka: KafkaService,
  ) {}

  async onModuleInit() {
    console.log('Outbox Publisher started');

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

        console.log(
          `Outbox event ${event.id} published`,
        );
      } catch (error) {
        console.error(
          `Failed to publish outbox event ${event.id}`,
          error,
        );
      }
    }
  }
}