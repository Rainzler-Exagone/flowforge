import { Module } from '@nestjs/common';
import { JobsModule } from './jobs/job.module';
import { KafkaModule } from 'libs/kafka/src';


@Module({
  imports: [JobsModule,KafkaModule],
  controllers: [],
  providers: [],
})
export class ApiModule {}
