import {
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { Consumer, Kafka } from 'kafkajs';
import { ImageWorkerService } from './image-worker.service';
import { jobs } from './db/schema';
import { db } from './db';
import { eq } from 'drizzle-orm';
import { PinoLogger } from 'pino-nestjs';


@Injectable()
export class KafkaConsumerService
implements OnModuleInit, OnModuleDestroy {


  constructor(
        private readonly imageWorker: ImageWorkerService,
        private readonly logger: PinoLogger
  ) { }
  private readonly kafka = new Kafka({
    clientId: 'flowforge-image-worker',
    brokers: [
      process.env.KAFKA_BROKER ?? 'localhost:9092',
    ],
  });

  private readonly consumer: Consumer =
    this.kafka.consumer({
      groupId: 'image-workers',
    });

  async onModuleInit() {
    await this.consumer.connect();

    await this.consumer.subscribe({
      topic: 'flowforge.jobs',
      fromBeginning: false,
    });

    await this.consumer.run({

      eachMessage: async ({ message }) => {
        const value = message.value?.toString();

        if (!value) {
          return;
        }

        const { jobId } = JSON.parse(value);

        this.logger.info(
          { jobId },
          'Received job from kafka',
        );
        const [job] = await db
          .select()
          .from(jobs)
          .where(eq(jobs.id, jobId))
          .limit(1);

        if (!job) {
          console.error(`Job ${jobId} not found`);
          return;
        }

        await this.imageWorker.processJob(job);
      },

    });

    this.logger.info(
      'Image worker kafka consumer connected',
    );
  }

  async onModuleDestroy() {
    await this.consumer.disconnect();
  }
}