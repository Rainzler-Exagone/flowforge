import { Module } from '@nestjs/common';
import { ImageWorkerService } from './image-worker.service';
import { KafkaConsumerService } from './kafka.consumer';
import { KafkaModule } from 'libs/kafka/src';

@Module({
  imports: [KafkaModule],
  controllers: [],
  providers: [ImageWorkerService, KafkaConsumerService],
})
export class ImageWorkerModule { }
