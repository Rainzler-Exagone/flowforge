import { Global, Module } from '@nestjs/common';
import { KafkaService } from './kafka.service';
import { LoggingModule } from 'apps/api/src/logging/logging.module';

@Global()
@Module({
  imports: [LoggingModule],
  providers: [KafkaService],
  exports: [KafkaService],
})
export class KafkaModule { }