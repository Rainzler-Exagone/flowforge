import { Module } from '@nestjs/common';
import { ImageWorkerService } from './image-worker.service';
import { KafkaConsumerService } from './kafka.consumer';

@Module({
  imports: [],
  controllers: [],
  providers: [ImageWorkerService,KafkaConsumerService],
})
export class ImageWorkerModule {}
