import {
    Injectable,
    OnModuleDestroy,
    OnModuleInit,
} from '@nestjs/common';
import { Kafka, Producer } from 'kafkajs';

@Injectable()
export class KafkaService
    implements OnModuleInit, OnModuleDestroy {
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

        console.log('Kafka producer connected');
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