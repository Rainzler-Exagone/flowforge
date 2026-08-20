import { Module } from '@nestjs/common';
import { JobsModule } from './jobs/job.module';
import { KafkaModule } from 'libs/kafka/src';
import { OutboxPublisherService } from './outbox/outbox-publisher.service';


@Module({
  imports: [JobsModule,KafkaModule],
  controllers: [],
  providers: [OutboxPublisherService],
})
export class ApiModule {}
