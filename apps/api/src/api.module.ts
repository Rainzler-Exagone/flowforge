import { Module } from '@nestjs/common';
import { JobsModule } from './jobs/job.module';
import { KafkaModule } from 'libs/kafka/src';
import { OutboxPublisherService } from './outbox/outbox-publisher.service';
import { DatabaseService } from './db/db.service';
import { LoggingModule } from './logging/logging.module';


@Module({
  imports:
    [
      LoggingModule, JobsModule, KafkaModule],
  controllers: [],
  providers: [OutboxPublisherService, DatabaseService],
})
export class ApiModule { }
