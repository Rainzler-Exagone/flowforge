import { Module } from '@nestjs/common';
import { ImageWorkerController } from './image-worker.controller';
import { ImageWorkerService } from './image-worker.service';

@Module({
  imports: [],
  controllers: [ImageWorkerController],
  providers: [ImageWorkerService],
})
export class ImageWorkerModule {}
