import {
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { Kafka, Producer } from 'kafkajs';
import { PinoLogger } from 'pino-nestjs';

@Injectable()
export class KafkaService
implements OnModuleInit, OnModuleDestroy {

  constructor(private readonly logger: PinoLogger) { }
  private readonly kafka = new Kafka({
    clientId: 'flowforge',
    brokers: [
      process.env.KAFKA_BROKER ?? 'localhost:9092',
    ],
  });



  private readonly producer: Producer =
    this.kafka.producer();

  async onModuleInit() {
    await this.producer.connect();

    this.logger.info(
      'Kafka producer connected',
    );
  }

  async onModuleDestroy() {
    await this.producer.disconnect();
  }

  async publish(
    topic: string,
    message: unknown,
    key?: string,
  ) {
    await this.producer.send({
      topic,
      messages: [
        {
          key,
          value: JSON.stringify(message),
        },
      ],
    });
  }
}